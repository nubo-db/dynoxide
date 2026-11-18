//! `eprintln!` panics when nobody reads stderr, and the release build aborts
//! on a panic, so the exit status a launcher relies on is lost. Everything in
//! `src/` writes to stderr through `dynoxide::stderr_line` instead; this
//! keeps it that way. Comments may still name the macro.

use std::path::Path;

fn rust_files(dir: &Path, found: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, found);
        } else if path.extension().is_some_and(|e| e == "rs") {
            found.push(path);
        }
    }
}

#[test]
fn nothing_in_src_writes_to_stderr_with_eprintln() {
    let mut files = Vec::new();
    rust_files(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    assert!(
        files.len() > 50,
        "found only {} files under src/",
        files.len()
    );

    let mut uses = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).unwrap();
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            if code.contains("eprintln!") || code.contains("eprint!") {
                uses.push(format!("{}:{}: {}", file.display(), n + 1, line.trim()));
            }
        }
    }
    assert!(
        uses.is_empty(),
        "use dynoxide::stderr_line, which can't panic, instead of:\n{}",
        uses.join("\n")
    );
}
