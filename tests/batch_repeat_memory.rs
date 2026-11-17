//! A BatchWriteItem whose `RequestItems` names one table thousands of times
//! behind a large member costs about what the same body with each name
//! written once costs. The entries are type-checked one at a time, so the
//! memory a request takes is bounded by the body, not by the number of
//! entries times the size of what was written ahead of them.
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

/// More than any request here needs, and far less than a request that copies
/// what it has read once for each entry would take.
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
async fn answer_and_peak(url: &str, body: String) -> (serde_json::Value, usize) {
    let client = reqwest::Client::new();
    let request = client
        .post(url)
        .header("x-amz-target", "DynamoDB_20120810.BatchWriteItem")
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
    let answer = request.send().await.unwrap().json().await.unwrap();
    (answer, PEAK.load(Ordering::SeqCst) - before)
}

/// A body with an 8 MiB member ahead of `RequestItems` and 4,000 entries,
/// each naming `table(i)`.
fn body(table: impl Fn(usize) -> String) -> String {
    let entries: Vec<String> = (0..4000)
        .map(|i| {
            format!(
                r#""{}":[{{"PutRequest":{{"Item":{{"pk":{{"S":"a"}}}}}}}}]"#,
                table(i)
            )
        })
        .collect();
    format!(
        r#"{{"ReturnConsumedCapacity":"{}","RequestItems":{{{}}}}}"#,
        "x".repeat(8 << 20),
        entries.join(",")
    )
}

#[tokio::test]
async fn a_table_repeated_behind_a_large_member_costs_what_the_body_costs() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = dynoxide::Database::memory().unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (answer, once) = answer_and_peak(&url, body(|i| format!("t{i:04}"))).await;
    assert_eq!(
        answer["__type"],
        "com.amazon.coral.validate#ValidationException"
    );

    let (answer, repeated) = answer_and_peak(&url, body(|_| "t".to_string())).await;
    assert_eq!(
        answer["Message"], "Duplicate key found in payload",
        "{answer}"
    );
    println!("peak beyond the idle process: {once} bytes with each name once, {repeated} repeated");
    assert!(
        repeated <= once + (8 << 20),
        "a repeated table took {repeated} bytes, where each name written once took {once}"
    );
}
