//! A BatchWriteItem carrying a member the operation doesn't have costs the
//! same wherever the member is written. A member written ahead of
//! `RequestItems` is read before the map, and only the members whose type is
//! checked are kept for it, so a large stray one is skipped as it is read,
//! not built a second time beside the parsed body.
//!
//! This test is a binary of its own because it counts every allocation the
//! process makes, and refuses any that would take the process past a cap, so
//! a regression fails here instead of exhausting the machine. The counters
//! are shared by the whole process, so this file must hold one test: another
//! running beside it would add its allocations to the ones measured here.

#![cfg(feature = "http-server")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Bytes allocated and not yet freed, and the most there have been.
static IN_USE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Far more than any request here needs.
const CAP: usize = 1 << 30;

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let now = IN_USE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
        if now > CAP {
            IN_USE.fetch_sub(layout.size(), Ordering::SeqCst);
            return std::ptr::null_mut();
        }
        PEAK.fetch_max(now, Ordering::SeqCst);
        // SAFETY: the layout is passed on unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        IN_USE.fetch_sub(layout.size(), Ordering::SeqCst);
        // SAFETY: the pointer came from `alloc` with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The answer to `body`, and the most memory the process held beyond what it
/// held before the request was sent.
async fn answer_and_peak(
    url: &str,
    operation: &str,
    body: String,
) -> (u16, serde_json::Value, usize) {
    let client = reqwest::Client::new();
    let request = client
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/eu-west-2/dynamodb/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body);
    let before = IN_USE.load(Ordering::SeqCst);
    PEAK.store(before, Ordering::SeqCst);
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let answer = response.json().await.unwrap();
    (status, answer, PEAK.load(Ordering::SeqCst) - before)
}

/// A body with a stray member of 200,000 members written ahead of
/// `RequestItems` or after it.
fn body(ahead: bool) -> String {
    let members: Vec<String> = (0..200_000).map(|i| format!(r#""m{i}":1"#)).collect();
    let stray = format!(r#""Stray":{{{}}}"#, members.join(","));
    let map = r#""RequestItems":{"stray":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]}"#;
    if ahead {
        format!("{{{stray},{map}}}")
    } else {
        format!("{{{map},{stray}}}")
    }
}

#[tokio::test]
async fn a_stray_member_ahead_of_the_map_costs_what_it_costs_after_it() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = dynoxide::Database::memory().unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (status, answer, _) = answer_and_peak(
        &url,
        "CreateTable",
        r#"{"TableName":"stray","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST"}"#.to_string(),
    )
    .await;
    assert_eq!(status, 200, "{answer}");

    // The stray member is not type-checked, so either way the request carries
    // on and the put is written.
    let written = serde_json::json!({"UnprocessedItems": {}});
    let mut peaks = Vec::new();
    for ahead in [false, true] {
        let (status, answer, peak) = answer_and_peak(&url, "BatchWriteItem", body(ahead)).await;
        assert_eq!((status, &answer), (200, &written), "written ahead: {ahead}");
        peaks.push(peak);
    }
    let (after, ahead) = (peaks[0], peaks[1]);
    println!("peak beyond the idle process: {ahead} bytes written ahead, {after} after");
    assert!(
        ahead <= after + (4 << 20),
        "a stray member ahead of the map took {ahead} bytes, where after it took {after}"
    );
    // Measured at about 24 MB for each placement, where reading the stray
    // member into a second value beside the parsed body took about 44 MB.
    // The bound leaves a third again as headroom and still fails if either
    // placement builds the member twice.
    const BOUND: usize = 32 << 20;
    for (placement, peak) in [("ahead of", ahead), ("after", after)] {
        assert!(
            peak <= BOUND,
            "a stray member written {placement} the map took {peak} bytes, more than {BOUND}"
        );
    }
}
