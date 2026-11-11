//! DynamoDB's reader for the base64 text of a `B` value, or a member of a
//! `BS`, as a request carries it (captured eu-west-2, 2026-11-11).
//!
//! It is not any configuration of the `base64` crate. With `n` the text's
//! length in UTF-8 bytes:
//!
//! 1. `n = 0` is an empty binary.
//! 2. `n = 1` is refused as too short.
//! 3. `==` on its own is refused with no message.
//! 4. Any other `n` that isn't a multiple of 4 is padded with `=` to one. If
//!    that is strict, canonical base64 it reads as that; otherwise the length
//!    is refused. So `QQ`, `QQ=` and `QUI` read, and `ab` and `-Q` don't.
//! 5. A multiple of 4 goes through a looser reader. Up to two trailing `=` are
//!    padding, and with padding the last four characters are the final group.
//!    A byte below `(` or above DEL anywhere is refused with no message. Any
//!    other character outside the alphabet reads as -1: refused with no
//!    message in a full group, and let through in the final group unless it is
//!    the last character before the padding, which must also leave no unused
//!    bits set. The final group's bytes come from arithmetic shifts on those
//!    values, so `-Q==` reads as `fd`.
//!
//! Encoding on the way out stays canonical padded base64.

use std::borrow::Cow;

/// Why DynamoDB refuses a value's base64 text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// One byte long.
    TooShort,
    /// A length that isn't a multiple of 4 and doesn't pad out to strict
    /// base64, in UTF-8 bytes.
    Length(usize),
    /// The last character before the padding leaves unused bits set, or isn't
    /// in the alphabet.
    LastNonPad,
    /// Refused with no message.
    NoMessage,
}

impl Refusal {
    /// The `SerializationException` message DynamoDB gives, empty for none.
    pub(crate) fn message(self) -> Cow<'static, str> {
        match self {
            Self::TooShort => Cow::Borrowed("Buffer is too short for base64 encoding"),
            Self::Length(n) => Cow::Owned(format!(
                "Base64 encoded length is expected a multiple of 4 bytes but found: {n}"
            )),
            // DynamoDB's spelling.
            Self::LastNonPad => Cow::Borrowed("Invalid last non-pad Base64 character dectected"),
            Self::NoMessage => Cow::Borrowed(""),
        }
    }
}

/// Whether DynamoDB reads `text` as base64, without decoding it.
pub(crate) fn check(text: &str) -> Result<(), Refusal> {
    read(text.as_bytes(), None)
}

/// The bytes DynamoDB reads `text` as.
pub(crate) fn decode(text: &str) -> Result<Vec<u8>, Refusal> {
    let mut bytes = Vec::with_capacity(text.len() / 4 * 3 + 2);
    read(text.as_bytes(), Some(&mut bytes))?;
    Ok(bytes)
}

fn read(text: &[u8], out: Option<&mut Vec<u8>>) -> Result<(), Refusal> {
    match text.len() {
        0 => Ok(()),
        1 => Err(Refusal::TooShort),
        _ if text == b"==" => Err(Refusal::NoMessage),
        n if n % 4 != 0 => read_unpadded(text, out).ok_or(Refusal::Length(n)),
        _ => read_padded(text, out),
    }
}

/// A character's value in the standard alphabet, or -1.
fn sextet(byte: u8) -> i32 {
    match byte {
        b'A'..=b'Z' => i32::from(byte - b'A'),
        b'a'..=b'z' => i32::from(byte - b'a') + 26,
        b'0'..=b'9' => i32::from(byte - b'0') + 52,
        b'+' => 62,
        b'/' => 63,
        _ => -1,
    }
}

/// The bits of the last character before `pads` padding characters that no
/// byte uses.
fn unused_bits(pads: usize) -> i32 {
    if pads == 2 { 0x0F } else { 0x03 }
}

/// Write a group of two, three or four values as one, two or three bytes. The
/// shifts are arithmetic and each byte is truncated, so a -1 in a final group
/// reads as DynamoDB reads it.
fn push_group(values: &[i32], out: &mut Vec<u8>) {
    out.push(((values[0] << 2) | (values[1] >> 4)) as u8);
    if values.len() > 2 {
        out.push(((values[1] << 4) | (values[2] >> 2)) as u8);
    }
    if values.len() > 3 {
        out.push(((values[2] << 6) | values[3]) as u8);
    }
}

/// A length that isn't a multiple of 4, read as if padded to one: `None`
/// unless that is strict, canonical base64.
fn read_unpadded(text: &[u8], out: Option<&mut Vec<u8>>) -> Option<()> {
    let written_pads = text.iter().rev().take_while(|&&b| b == b'=').count();
    let pads = written_pads + (4 - text.len() % 4);
    if pads > 2 {
        return None;
    }
    let data = &text[..text.len() - written_pads];
    let mut values = [0i32; 4];
    let mut out = out;
    for group in data.chunks(4) {
        for (value, &byte) in values.iter_mut().zip(group) {
            *value = sextet(byte);
            if *value < 0 {
                return None;
            }
        }
        if group.len() < 4 && values[group.len() - 1] & unused_bits(pads) != 0 {
            return None;
        }
        if let Some(out) = out.as_deref_mut() {
            push_group(&values[..group.len()], out);
        }
    }
    Some(())
}

/// A multiple of 4, by the looser reader.
fn read_padded(text: &[u8], out: Option<&mut Vec<u8>>) -> Result<(), Refusal> {
    if text.iter().any(|&b| !(b'('..=0x7F).contains(&b)) {
        return Err(Refusal::NoMessage);
    }
    let pads = text
        .iter()
        .rev()
        .take(2)
        .take_while(|&&b| b == b'=')
        .count();
    let full = if pads > 0 { text.len() - 4 } else { text.len() };
    let mut values = [0i32; 4];
    let mut out = out;
    for group in text[..full].chunks(4) {
        for (value, &byte) in values.iter_mut().zip(group) {
            *value = sextet(byte);
            if *value < 0 {
                return Err(Refusal::NoMessage);
            }
        }
        if let Some(out) = out.as_deref_mut() {
            push_group(&values, out);
        }
    }
    if pads == 0 {
        return Ok(());
    }
    let kept = 4 - pads;
    for (value, &byte) in values.iter_mut().zip(&text[full..full + kept]) {
        *value = sextet(byte);
    }
    let last = values[kept - 1];
    if last < 0 || last & unused_bits(pads) != 0 {
        return Err(Refusal::LastNonPad);
    }
    if let Some(out) = out {
        push_group(&values[..kept], out);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    /// The outcome class of reading `text`: `Ok` or the refusal.
    fn class(text: &str) -> Result<(), Refusal> {
        let checked = check(text);
        assert_eq!(
            checked,
            decode(text).map(drop),
            "check and decode disagree on {text:?}"
        );
        checked
    }

    /// Characters as inclusive ranges.
    type Ranges = &'static [(u8, u8)];

    /// Captured eu-west-2, 2026-11-11: each of the 128 ASCII characters put
    /// in six places. For each shape, the characters DynamoDB accepted and the
    /// ones it refused with `Invalid last non-pad Base64 character dectected`;
    /// every other character was refused with no message.
    const ASCII_SWEEP: &[(&str, Ranges, Ranges)] = &[
        (
            "Qc==",
            &[(0x41, 0x41), (0x51, 0x51), (0x67, 0x67), (0x77, 0x77)],
            &[
                (0x28, 0x40),
                (0x42, 0x50),
                (0x52, 0x66),
                (0x68, 0x76),
                (0x78, 0x7F),
            ],
        ),
        ("cQ==", &[(0x28, 0x7F)], &[]),
        (
            "QQc=",
            &[
                (0x30, 0x30),
                (0x34, 0x34),
                (0x38, 0x38),
                (0x3D, 0x3D),
                (0x41, 0x41),
                (0x45, 0x45),
                (0x49, 0x49),
                (0x4D, 0x4D),
                (0x51, 0x51),
                (0x55, 0x55),
                (0x59, 0x59),
                (0x63, 0x63),
                (0x67, 0x67),
                (0x6B, 0x6B),
                (0x6F, 0x6F),
                (0x73, 0x73),
                (0x77, 0x77),
            ],
            &[
                (0x28, 0x2F),
                (0x31, 0x33),
                (0x35, 0x37),
                (0x39, 0x3C),
                (0x3E, 0x40),
                (0x42, 0x44),
                (0x46, 0x48),
                (0x4A, 0x4C),
                (0x4E, 0x50),
                (0x52, 0x54),
                (0x56, 0x58),
                (0x5A, 0x62),
                (0x64, 0x66),
                (0x68, 0x6A),
                (0x6C, 0x6E),
                (0x70, 0x72),
                (0x74, 0x76),
                (0x78, 0x7F),
            ],
        ),
        ("QcQ=", &[(0x28, 0x7F)], &[]),
        ("cQQ=", &[(0x28, 0x7F)], &[]),
        (
            "cQQQ",
            &[(0x2B, 0x2B), (0x2F, 0x39), (0x41, 0x5A), (0x61, 0x7A)],
            &[],
        ),
    ];

    #[test]
    fn every_ascii_character_in_six_places_reads_as_captured() {
        let within =
            |ranges: &[(u8, u8)], c: u8| ranges.iter().any(|&(lo, hi)| (lo..=hi).contains(&c));
        let mut wrong = Vec::new();
        for &(shape, accepted, last_non_pad) in ASCII_SWEEP {
            for c in 0u8..=0x7F {
                let text = shape.replace('c', &char::from(c).to_string());
                let expected = if within(accepted, c) {
                    Ok(())
                } else if within(last_non_pad, c) {
                    Err(Refusal::LastNonPad)
                } else {
                    Err(Refusal::NoMessage)
                };
                let got = class(&text);
                if got != expected {
                    wrong.push(format!("{text:?}: expected {expected:?}, got {got:?}"));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of 768 differ:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
    }

    /// Captured eu-west-2, 2026-11-11: the lenient forms DynamoDB accepted,
    /// and the canonical base64 it returned for each when scanned.
    const STORED: &[(&str, &str)] = &[
        ("QQ", "QQ=="),
        ("QQ=", "QQ=="),
        ("QQ==", "QQ=="),
        ("QUI", "QUI="),
        ("QUI=", "QUI="),
        ("abc", "abc="),
        ("abcdefg", "abcdefg="),
        ("QQQQQQ", "QQQQQQ=="),
        ("abcdQUI", "abcdQUI="),
        ("abcdQQ=", "abcdQQ=="),
        ("-Q==", "/Q=="),
        ("{Q==", "/Q=="),
        ("*Q==", "/Q=="),
        ("(Q==", "/Q=="),
        ("~Q==", "/Q=="),
        ("\u{7f}Q==", "/Q=="),
        ("=Q==", "/Q=="),
        ("Q-Q=", "//Q="),
        ("Q=Q=", "//Q="),
        ("-QQ=", "/QQ="),
        ("QQQQ-Q==", "QQQQ/Q=="),
        ("QQQQQ-Q=", "QQQQ//Q="),
        ("--Q=", "//Q="),
        ("-w==", "/w=="),
        ("-/Q=", "//Q="),
        ("/-Q=", "//Q="),
    ];

    #[test]
    fn lenient_forms_read_as_the_bytes_dynamodb_stored() {
        for &(sent, stored) in STORED {
            assert_eq!(
                decode(sent).map(|bytes| STANDARD.encode(bytes)),
                Ok(stored.to_string()),
                "{sent:?}"
            );
        }
    }

    #[test]
    fn the_two_refused_writes_of_the_stored_bytes_capture_are_refused() {
        assert_eq!(decode("Q--="), Err(Refusal::LastNonPad));
        assert_eq!(decode(" "), Err(Refusal::TooShort));
    }

    #[test]
    fn every_canonical_padded_value_reads_as_the_base64_crate_reads_it() {
        // Every byte string up to two long, and a spread of longer ones.
        let mut inputs: Vec<Vec<u8>> = vec![Vec::new()];
        for len in 1..=2usize {
            let count = 1usize << (8 * len);
            inputs.extend((0..count).map(|i| (0..len).map(|k| (i >> (8 * k)) as u8).collect()));
        }
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        for len in 3..=8usize {
            for _ in 0..2000 {
                inputs.push(
                    (0..len)
                        .map(|_| {
                            seed ^= seed << 13;
                            seed ^= seed >> 7;
                            seed ^= seed << 17;
                            seed as u8
                        })
                        .collect(),
                );
            }
        }
        for bytes in inputs {
            let encoded = STANDARD.encode(&bytes);
            assert_eq!(STANDARD.decode(&encoded).unwrap(), bytes);
            assert_eq!(decode(&encoded), Ok(bytes), "{encoded}");
        }
    }

    #[test]
    fn refusals_are_worded_as_dynamodb_words_them() {
        assert_eq!(
            Refusal::TooShort.message(),
            "Buffer is too short for base64 encoding"
        );
        assert_eq!(
            Refusal::Length(7).message(),
            "Base64 encoded length is expected a multiple of 4 bytes but found: 7"
        );
        assert_eq!(
            Refusal::LastNonPad.message(),
            "Invalid last non-pad Base64 character dectected"
        );
        assert_eq!(Refusal::NoMessage.message(), "");
    }

    #[test]
    fn length_counts_utf8_bytes() {
        assert_eq!(check("\u{e9}"), Err(Refusal::Length(2)));
        assert_eq!(check("ab\u{100}d"), Err(Refusal::Length(5)));
        assert_eq!(check("\u{1F600}bcd"), Err(Refusal::Length(7)));
        assert_eq!(check("\u{e9}=="), Err(Refusal::NoMessage));
    }
}
