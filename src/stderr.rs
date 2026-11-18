//! Writing a line to stderr without panicking.
//!
//! In every build, because the import report and the binary's errors go
//! through it as well as the servers' startup lines. `tests/no_eprintln.rs`
//! keeps `eprintln!` out of `src/`.

use std::io::Write;

/// Writes a line to stderr, ignoring a failed write.
///
/// `eprintln!` panics when the write fails, and stderr fails when nobody reads
/// it: a launcher that read it through a pipe has been killed, or never kept
/// the pipe open. The release build aborts on a panic, so the database would
/// never close and the exit status a caller relies on would be lost. A line
/// nobody can read isn't worth that.
#[doc(hidden)]
pub fn stderr_line(args: std::fmt::Arguments<'_>) {
    // One lock for the whole line, as `eprintln!` takes, so a log line on
    // stderr can't land between the message and its newline.
    write_line(&mut std::io::stderr().lock(), args);
}

fn write_line(out: &mut impl Write, args: std::fmt::Arguments<'_>) {
    let _ = writeln!(out, "{args}");
}

#[cfg(test)]
mod stderr_line_tests {
    use super::write_line;
    use std::io::{self, Write};

    /// Fails every write, as stderr does once its reader has gone.
    struct BrokenPipe;

    impl Write for BrokenPipe {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    #[test]
    fn a_line_nobody_can_read_is_dropped_without_a_panic() {
        write_line(&mut BrokenPipe, format_args!("Shutting down..."));
    }

    #[test]
    fn a_line_with_a_reader_arrives_whole() {
        let mut out = Vec::new();
        write_line(&mut out, format_args!("listening on {}", 8000));
        assert_eq!(out, b"listening on 8000\n");
    }
}
