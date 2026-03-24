use dynoxide_benchmarks::memory_measurement::format_bytes;
use std::process::Command;

fn main() {
    println!("=== Dynoxide vs DynamoDB Local vs LocalStack: Size Comparison ===\n");

    let dynoxide_size = measure_dynoxide_binary();
    let ddb_local_size = measure_docker_image("amazon/dynamodb-local:latest");
    let localstack_size = measure_docker_image("localstack/localstack:latest");

    println!("{:<25} {:>15}", "Target", "Install Size");
    println!("{:-<25} {:-<15}", "", "");

    if let Some(size) = dynoxide_size {
        println!("{:<25} {:>15}", "Dynoxide (binary)", format_bytes(size));
    } else {
        println!(
            "{:<25} {:>15}",
            "Dynoxide (binary)", "(build with --release first)"
        );
    }

    if let Some(size) = ddb_local_size {
        println!(
            "{:<25} {:>15}",
            "DynamoDB Local (Docker)",
            format_bytes(size)
        );
    } else {
        println!(
            "{:<25} {:>15}",
            "DynamoDB Local (Docker)", "(docker pull first)"
        );
    }

    if let Some(size) = localstack_size {
        println!("{:<25} {:>15}", "LocalStack (Docker)", format_bytes(size));
    } else {
        println!(
            "{:<25} {:>15}",
            "LocalStack (Docker)", "(docker pull first)"
        );
    }

    // Print ratios
    if let (Some(dyn_size), Some(ddb_size)) = (dynoxide_size, ddb_local_size) {
        println!(
            "\nDynamoDB Local is {:.0}x larger than Dynoxide",
            ddb_size as f64 / dyn_size as f64
        );
    }
    if let (Some(dyn_size), Some(ls_size)) = (dynoxide_size, localstack_size) {
        println!(
            "LocalStack is {:.0}x larger than Dynoxide",
            ls_size as f64 / dyn_size as f64
        );
    }

    // Output JSON for report generation
    println!("\n--- JSON Output ---");
    println!(
        "{}",
        serde_json::json!({
            "dynoxide_binary_bytes": dynoxide_size,
            "dynamodb_local_docker_bytes": ddb_local_size,
            "localstack_docker_bytes": localstack_size,
        })
    );
}

fn measure_dynoxide_binary() -> Option<u64> {
    // Look for the release binary in the parent crate's target directory
    let paths = ["../target/release/dynoxide", "target/release/dynoxide"];

    for path in &paths {
        if let Ok(meta) = std::fs::metadata(path) {
            return Some(meta.len());
        }
    }

    // Try building and measuring
    eprintln!(
        "Note: Release binary not found. Run `cargo build --release` in the root crate first."
    );
    None
}

fn measure_docker_image(image: &str) -> Option<u64> {
    let output = Command::new("docker")
        .args(["image", "inspect", image, "--format", "{{.Size}}"])
        .output()
        .ok()?;

    if !output.status.success() {
        eprintln!("Note: Docker image {image} not found. Run `docker pull {image}` first.");
        return None;
    }

    let size_str = String::from_utf8_lossy(&output.stdout);
    size_str.trim().parse::<u64>().ok()
}
