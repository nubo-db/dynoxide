//! Byte-level helpers for walking and rewriting a request body as written.
//!
//! The readers that rewrite part of a body before it is decoded (values too
//! deep for the parser, forms DynamoDB reads by position) work on the text
//! itself, never on a parsed value written back out, which would reorder
//! members and change how numbers are written. These are the pieces they share.

/// A value's span in a body: its first byte and the byte after its last.
pub(crate) type Span = (usize, usize);

/// The end of the JSON string whose opening quote is at `start`: the byte
/// after its closing quote, or `None` when the body ends first. An escaped
/// character, an escaped quote or backslash included, is stepped over whole.
pub(crate) fn string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// `body` with each span replaced by its text. The spans are in order of
/// their start; one that starts inside a span already replaced went with that
/// span and is skipped.
pub(crate) fn splice<T: AsRef<str>>(
    body: &str,
    replacements: impl IntoIterator<Item = (Span, T)>,
) -> String {
    let mut out = String::with_capacity(body.len());
    let mut copied = 0;
    for ((start, end), text) in replacements {
        if start < copied {
            continue;
        }
        out.push_str(&body[copied..start]);
        out.push_str(text.as_ref());
        copied = end;
    }
    out.push_str(&body[copied..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_string_ends_after_its_closing_quote_whatever_it_escapes() {
        let body = br#""a\"b\\" tail"#;
        assert_eq!(string_end(body, 0), Some(8));
        assert_eq!(string_end(br#""open"#, 0), None);
    }

    #[test]
    fn a_span_inside_one_already_replaced_is_skipped() {
        let body = "0123456789";
        assert_eq!(
            splice(body, [((1, 6), "x"), ((2, 3), "y"), ((7, 8), "z")]),
            "0x6z89"
        );
        assert_eq!(splice(body, Vec::<(Span, &str)>::new()), body);
    }
}
