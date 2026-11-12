//! Number tokens DynamoDB reads that JSON doesn't.
//!
//! DynamoDB's request parser takes any run of digits, `.`, `e`, `E`, `+` and
//! `-` that starts with a digit or `-` as a number token, and reads its text
//! only when the member it sits in is read (captured eu-west-2, 9 and 10
//! November 2026):
//!
//! - in a member it doesn't read, any such token is accepted;
//! - in a member of another type it is named by how it is written,
//!   `DECIMAL_VALUE` when it holds `.`, `e` or `E` and `NUMBER_VALUE`
//!   otherwise, as a well-formed number is;
//! - in an integer member it is read by a looser rule than JSON's (see
//!   [`read_integer`]), and refused as `Bad number format` when that fails.
//!
//! serde_json refuses the whole body on such a token, so before the body is
//! parsed each one is swapped for a stand-in number of the same kind
//! ([`stand_in_malformed_numbers`]), and the token's text is kept in a table
//! the integer readers consult for the request in hand. A stand-in is
//! negative and its digits appear nowhere else in the body, so a reader that
//! doesn't consult the table and quotes the number it was given can be caught
//! ([`quotes_stand_in`]) and answered as the token was before: a bare
//! `SerializationException`.

use std::borrow::Cow;
use std::rc::Rc;

/// What DynamoDB answers for a number token an integer member can't read.
pub(crate) const BAD_NUMBER_FORMAT: &str = "Bad number format";

/// The stand-ins in one request body, each with the token it replaced.
///
/// A body can hold millions of tokens, so each stand-in costs one small entry
/// pointing into a copy of the body as sent, rather than a copy of its token.
#[derive(Debug)]
pub(crate) struct StandIns {
    /// The body as the client sent it.
    sent: Box<str>,
    /// One entry per stand-in, in rising order of digits.
    entries: Vec<StandIn>,
}

/// A stand-in: `-<digits>` for a whole token, `-<digits>.5` for one holding
/// `.`, `e` or `E`, in place of the token at `start..end` of the body sent.
#[derive(Debug)]
struct StandIn {
    digits: i64,
    start: u32,
    end: u32,
}

impl StandIns {
    /// The token a number stands in for, or `None` for a number the client
    /// sent.
    fn token(&self, number: &serde_json::Number) -> Option<&str> {
        let digits = if let Some(whole) = number.as_i64() {
            whole.checked_neg()?
        } else if number.is_f64() {
            // Exact for a stand-in: half a unit is a multiple of the spacing
            // between f64s this size.
            let digits = -number.as_f64()? - 0.5;
            if digits.fract() != 0.0 || !(0.0..1e10).contains(&digits) {
                return None;
            }
            digits as i64
        } else {
            return None;
        };
        let token = self.token_written_with(digits)?;
        // `-<digits>` and `-<digits>.5` are different numbers, and only one of
        // them is a stand-in.
        (token.contains(['.', 'e', 'E']) == number.is_f64()).then_some(token)
    }

    /// The token the stand-in written with `digits` replaced, if one was.
    fn token_written_with(&self, digits: i64) -> Option<&str> {
        let at = self
            .entries
            .binary_search_by_key(&digits, |entry| entry.digits)
            .ok()?;
        let entry = &self.entries[at];
        Some(&self.sent[entry.start as usize..entry.end as usize])
    }
}

thread_local! {
    /// The stand-ins of the request being answered on this thread, if it has
    /// any. Set around the request by the wire seam; empty outside one.
    static STAND_INS: std::cell::RefCell<Option<Rc<StandIns>>> =
        const { std::cell::RefCell::new(None) };
}

/// The token a number written as `text` stands in for, when the request in
/// hand holds stand-ins and this is one of them.
fn token_for(text: &str) -> Option<String> {
    STAND_INS.with(|cell| {
        let stand_ins = cell.borrow();
        let stand_ins = stand_ins.as_ref()?;
        let number = serde_json::from_str::<serde_json::Number>(text).ok()?;
        stand_ins.token(&number).map(str::to_string)
    })
}

/// Whether the error answer `json` quotes a stand-in in one of its messages,
/// so that it would give away a number the client never sent. Only the
/// messages are read: the rest of an answer can hold stored items, whose
/// digits a stand-in isn't chosen to avoid. Always false outside a request
/// with stand-ins.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) fn error_quotes_stand_in(json: &str) -> bool {
    if STAND_INS.with(|cell| cell.borrow().is_none()) {
        return false;
    }
    let Ok(serde_json::Value::Object(error)) = serde_json::from_str(json) else {
        return false;
    };
    let message = |value: &serde_json::Value| {
        ["message", "Message"]
            .into_iter()
            .filter_map(|key| value.get(key)?.as_str())
            .any(quotes_stand_in)
    };
    let error = serde_json::Value::Object(error);
    message(&error)
        || error
            .get("CancellationReasons")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|reasons| reasons.iter().any(message))
}

/// Whether `text` quotes a stand-in. Always false outside a request with
/// stand-ins.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
fn quotes_stand_in(text: &str) -> bool {
    STAND_INS.with(|cell| {
        cell.borrow().as_ref().is_some_and(|stand_ins| {
            let mut quoted = false;
            for_each_ten_digits(text, |digits| {
                quoted |= stand_ins.token_written_with(digits).is_some();
            });
            quoted
        })
    })
}

/// Run `read` with `stand_ins` as the request's table, restoring the table in
/// force before however `read` ends. `read` must not await: the table belongs
/// to the thread.
#[cfg(any(feature = "http-server", test))]
pub(crate) fn with_stand_ins<T>(stand_ins: Option<StandIns>, read: impl FnOnce() -> T) -> T {
    with_table(stand_ins.map(Rc::new), read)
}

#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
fn with_table<T>(stand_ins: Option<Rc<StandIns>>, read: impl FnOnce() -> T) -> T {
    struct Restore(Option<Rc<StandIns>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            STAND_INS.with(|cell| *cell.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(STAND_INS.with(|cell| cell.replace(stand_ins)));
    read()
}

/// Run `future` with `stand_ins` as the request's table, set around each of
/// its polls and restored after, so the table holds across the future's
/// awaits without reaching anything polled in between. The wasm engine's
/// single thread runs other requests between those polls.
#[cfg(any(feature = "wasm-sqlite", test))]
pub(crate) async fn with_stand_ins_async<F: std::future::Future>(
    stand_ins: Option<StandIns>,
    future: F,
) -> F::Output {
    struct Tabled<F> {
        stand_ins: Option<Rc<StandIns>>,
        future: std::pin::Pin<Box<F>>,
    }
    impl<F: std::future::Future> std::future::Future for Tabled<F> {
        type Output = F::Output;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<F::Output> {
            let stand_ins = self.stand_ins.clone();
            with_table(stand_ins, || self.future.as_mut().poll(cx))
        }
    }
    Tabled {
        stand_ins: stand_ins.map(Rc::new),
        future: Box::pin(future),
    }
    .await
}

/// `body` with each number token that isn't valid JSON replaced by a stand-in
/// number of the same kind, and the table of what each stand-in replaced; or
/// `None`, without copying the body, when every token is valid JSON.
///
/// A token is the maximal run of `-`, `+`, `.`, `e`, `E` and digits starting
/// at a `-` or a digit outside a string, as [`crate::serde_errors`] reads
/// them. A token that follows JSON's number grammar is left alone whatever
/// its size. One starting with `.` or `+`, or running into a letter, is left
/// for the parser to refuse, as DynamoDB refuses it.
///
/// A body past about 890 MB, too long for every stand-in to be written with
/// ten digits, is also left for the parser to refuse.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) fn stand_in_malformed_numbers(body: &str) -> Option<(String, StandIns)> {
    let mut entries = Vec::new();
    for_each_token(body, |start, end| {
        if !crate::serde_errors::json_number(&body[start..end]) {
            // The length bound below keeps every offset within a u32.
            entries.push(StandIn {
                digits: 0,
                start: start as u32,
                end: end as u32,
            });
        }
    });
    if entries.is_empty() {
        return None;
    }

    // Each candidate tried is either taken by a malformed token or passed
    // over for a run of digits or a number the body holds, so no more than
    // three per byte are tried, and only those need recording.
    let last = i64::try_from(body.len())
        .ok()
        .and_then(|len| len.checked_mul(3))
        .and_then(|tried| FIRST_STAND_IN.checked_add(tried))
        .filter(|last| *last < 10_000_000_000)?;
    let candidate = |value: i64| (FIRST_STAND_IN..=last).contains(&value);

    // A stand-in's digits mustn't appear in the body, as written or in a
    // string once its escapes are read, so a message quoting them can only be
    // quoting the stand-in.
    let mut body_digits = std::collections::HashSet::new();
    let mut reserve = |text: &str| {
        for_each_ten_digits(text, |digits| {
            if candidate(digits) {
                body_digits.insert(digits);
            }
        });
    };
    reserve(body);
    for_each_escaped_string(body, |decoded| reserve(decoded));
    // Nor may it equal a number the client sent, by value, so a reader that
    // meets either after a round trip through `serde_json::Value` can tell
    // them apart.
    let mut whole_values = std::collections::HashSet::new();
    let mut decimal_values = std::collections::HashSet::new();
    for_each_token(body, |start, end| {
        if let Ok(number) = serde_json::from_str::<serde_json::Number>(&body[start..end]) {
            if let Some(whole) = number.as_i64() {
                if whole.checked_neg().is_some_and(candidate) {
                    whole_values.insert(whole);
                }
            } else if let Some(decimal) = number.as_f64() {
                let digits = -decimal - 0.5;
                if (FIRST_STAND_IN as f64..=last as f64).contains(&digits) {
                    decimal_values.insert(decimal.to_bits());
                }
            }
        }
    });

    let mut next = FIRST_STAND_IN;
    // A stand-in is at most 13 bytes: `-`, ten digits and `.5`.
    let mut out = String::with_capacity(body.len() + entries.len() * 13);
    let mut copied = 0;
    for entry in &mut entries {
        let (start, end) = (entry.start as usize, entry.end as usize);
        let token = &body[start..end];
        let decimal = token.contains(['.', 'e', 'E']);
        let (text, digits) = loop {
            let digits = next;
            next += 1;
            if body_digits.contains(&digits) {
                continue;
            }
            if decimal {
                let value = (-(digits as f64) - 0.5).to_bits();
                if !decimal_values.contains(&value) {
                    break (format!("-{digits}.5"), digits);
                }
            } else if !whole_values.contains(&-digits) {
                break (format!("-{digits}"), digits);
            }
        };
        out.push_str(&body[copied..start]);
        out.push_str(&text);
        // Keep the columns after a short token where they were.
        for _ in text.len()..token.len() {
            out.push(' ');
        }
        copied = end;
        entry.digits = digits;
    }
    out.push_str(&body[copied..]);
    Some((
        out,
        StandIns {
            sent: body.into(),
            entries,
        },
    ))
}

/// The first stand-in's digits. Its value is negative, so no reader takes it
/// for a count, and ten digits long, so it is unlikely to be in a body.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
const FIRST_STAND_IN: i64 = 7_310_000_001;

/// Call `each` with the value of every ten digits in a row in `text`,
/// strings included, as a run of more than ten holds several.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
fn for_each_ten_digits(text: &str, mut each: impl FnMut(i64)) {
    let mut run = 0;
    let mut value: i64 = 0;
    for byte in text.bytes() {
        if byte.is_ascii_digit() {
            value = value % 1_000_000_000 * 10 + i64::from(byte - b'0');
            run += 1;
            if run >= 10 {
                each(value);
            }
        } else {
            run = 0;
            value = 0;
        }
    }
}

/// Call `each` with the decoded text of every string in `body` that holds an
/// escape.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
fn for_each_escaped_string(body: &str, mut each: impl FnMut(&str)) {
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        let start = i;
        let mut escaped = false;
        i += 1;
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' {
                escaped = true;
                i += 2;
            } else {
                i += 1;
            }
        }
        if escaped
            && i < bytes.len()
            && let Ok(decoded) = serde_json::from_str::<String>(&body[start..=i])
        {
            each(&decoded);
        }
        i += 1;
    }
}

/// Call `token` with the start and end of each number token in `body`.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
fn for_each_token(body: &str, mut token: impl FnMut(usize, usize)) {
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                token(start, i);
            }
            // Letters run on, so `x1` and `e5` start no token: JSON has no
            // bare words beyond `true`, `false` and `null`.
            b'a'..=b'z' | b'A'..=b'Z' | b'+' | b'.' | b'_' => {
                while i < bytes.len()
                    && matches!(bytes[i], b'a'..=b'z' | b'A'..=b'Z' | b'+' | b'.' | b'_' | b'0'..=b'9' | b'-')
                {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
}

/// The text an integer member reads a number as: the number as written, or,
/// for a stand-in, the well-formed twin of the token it replaced. `Err` when
/// the token is one an integer member refuses.
///
/// DynamoDB's integer members read `-` alone, or a token with a sign, digits,
/// a point and an exponent where JSON is stricter: the integer part may have
/// leading zeros or be empty, and the point may have no digits after it, but
/// the mantissa needs a digit and an exponent needs one too. Such a token is
/// read as its twin: `-` as `0`, `01` as `1`, `-.5` as `-0.5`, `1.` as `1.0`,
/// `1.e5` as `1.0e5` (captured eu-west-2, 9 and 10 November 2026).
///
/// The text is owned only for a stand-in, which [`read_integer`] relies on.
pub(crate) fn integer_text(raw: &str) -> Result<Cow<'_, str>, ()> {
    match token_for(raw) {
        None => Ok(Cow::Borrowed(raw)),
        Some(token) => twin(&token).map(Cow::Owned).ok_or(()),
    }
}

/// The well-formed number an integer member reads `token` as, or `None` when
/// it refuses it.
fn twin(token: &str) -> Option<String> {
    if token == "-" {
        return Some("0".to_string());
    }
    let (sign, rest) = match token.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", token),
    };
    let digits = |text: &str| text.bytes().take_while(u8::is_ascii_digit).count();
    let int_len = digits(rest);
    let (int, rest) = rest.split_at(int_len);
    let (fraction, rest) = match rest.strip_prefix('.') {
        Some(after) => {
            let len = digits(after);
            (Some(&after[..len]), &after[len..])
        }
        None => (None, rest),
    };
    if int.is_empty() && fraction.is_none_or(str::is_empty) {
        return None;
    }
    let exponent = match rest.strip_prefix(['e', 'E']) {
        Some(after) => {
            let signed = after.strip_prefix(['+', '-']).unwrap_or(after);
            if digits(signed) == 0 || digits(signed) != signed.len() {
                return None;
            }
            rest
        }
        None if rest.is_empty() => "",
        None => return None,
    };
    let int = int.trim_start_matches('0');
    let mut twin = format!("{sign}{}", if int.is_empty() { "0" } else { int });
    if let Some(fraction) = fraction {
        twin.push('.');
        twin.push_str(if fraction.is_empty() { "0" } else { fraction });
    }
    twin.push_str(exponent);
    Some(twin)
}

/// An integer member's reading of a number: its value, truncated toward zero
/// and held wide enough to clamp to the member's range, and whether it was
/// read leniently: written as a fraction, past f64's range, or as a token JSON
/// doesn't allow. DynamoDB checks a member read leniently on its older
/// validation layer (`Limit` `-` and `00` answer `Value '0' at 'limit'`, as
/// `0.5` does; captured eu-west-2, 10 November 2026).
pub(crate) struct Integer {
    pub(crate) value: i128,
    pub(crate) lenient: bool,
}

/// Why an integer member couldn't read what it was given.
pub(crate) enum IntegerFault {
    /// A number token the member refuses: `Bad number format`.
    BadNumber,
    /// Not a number at all, with serde's reason.
    NotANumber(serde_json::Error),
}

/// Read `raw`, a JSON value's text, as an integer member reads it.
///
/// A fraction truncates toward zero and one past f64's range saturates, as
/// DynamoDB reads them (captured eu-west-2 and us-east-1, 6 November 2026).
pub(crate) fn read_integer(raw: &str) -> Result<Integer, IntegerFault> {
    let text = integer_text(raw).map_err(|()| IntegerFault::BadNumber)?;
    let from_stand_in = matches!(text, Cow::Owned(_));
    let number = match serde_json::from_str::<serde_json::Number>(&text) {
        Ok(number) => number,
        Err(e) if crate::serde_errors::is_number_out_of_range(&e) => {
            let value = if text.starts_with('-') {
                i128::MIN
            } else {
                i128::MAX
            };
            return Ok(Integer {
                value,
                lenient: true,
            });
        }
        Err(e) => return Err(IntegerFault::NotANumber(e)),
    };
    let whole = number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from));
    Ok(if let Some(value) = whole {
        Integer {
            value,
            lenient: from_stand_in,
        }
    } else {
        // NaN and the infinities cannot appear in JSON; `as` saturates.
        Integer {
            value: number.as_f64().unwrap_or(0.0).trunc() as i128,
            lenient: true,
        }
    })
}

/// The `SERIALIZATION:`-marked refusal of a number token an integer member
/// can't read, for a `Deserialize` impl to raise.
pub(crate) fn bad_number<E: serde::de::Error>() -> E {
    E::custom(format!(
        "{}{BAD_NUMBER_FORMAT}",
        crate::serde_errors::SERIALIZATION_MARKER
    ))
}

/// Deserialise a throughput unit (`ReadCapacityUnits`, `WriteCapacityUnits`,
/// `MaxReadRequestUnits`, `MaxWriteRequestUnits`), a Long, as an integer
/// member reads it: a fraction truncates and a number past the range
/// saturates (`0.5` and `-.5` read as 0, captured eu-west-2, 10 November
/// 2026). Anything other than a number keeps serde's own wording.
pub(crate) fn deserialize_units<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    use serde::Deserialize as _;
    use serde::de::Error as _;
    let Some(raw) = Option::<Box<serde_json::value::RawValue>>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match read_integer(raw.get()) {
        Ok(integer) => Ok(Some(saturate_i64(integer.value))),
        Err(IntegerFault::BadNumber) => Err(bad_number()),
        Err(IntegerFault::NotANumber(_)) => serde_json::from_str::<i64>(raw.get())
            .map(Some)
            .map_err(D::Error::custom),
    }
}

fn saturate_i64(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Read the throughput units in a `ProvisionedThroughput` object held as JSON
/// as [`deserialize_units`] does, writing each back as the whole number it
/// reads as, so every later check sees what DynamoDB reads. A token an integer
/// member refuses is `Bad number format`, raised as the deserialisation fault
/// it is, before anything checks the request. Any other member is left alone,
/// since DynamoDB doesn't read it (captured eu-west-2, 11 November 2026).
pub(crate) fn read_units_in<E: serde::de::Error>(
    throughput: &mut serde_json::Value,
) -> Result<(), E> {
    let Some(members) = throughput.as_object_mut() else {
        return Ok(());
    };
    for unit in ["ReadCapacityUnits", "WriteCapacityUnits"] {
        if let Some(value) = members.get_mut(unit)
            && let serde_json::Value::Number(number) = value
        {
            match read_integer(&number.to_string()) {
                Ok(integer) => *value = saturate_i64(integer.value).into(),
                Err(IntegerFault::BadNumber) => return Err(bad_number()),
                Err(IntegerFault::NotANumber(_)) => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replaced(body: &str) -> (String, StandIns) {
        stand_in_malformed_numbers(body).expect("a malformed token")
    }

    #[test]
    fn a_body_of_valid_numbers_is_not_copied() {
        for body in [
            r#"{"Limit":5,"X":1.5,"Y":-0,"Z":1e400,"W":18446744073709551616,"V":-1E-5}"#,
            r#"{"S":"1.5.5","K":"a\"1.5.5","1e":true,"L":[0,-1,2.5]}"#,
            // Tokens DynamoDB refuses outright stay for the parser to refuse.
            r#"{"A":.5}"#,
            r#"{"A":+1}"#,
            r#"{"A":0x10}"#,
            r#"{"A":1.5x}"#,
            "",
        ] {
            assert!(stand_in_malformed_numbers(body).is_none(), "{body}");
        }
    }

    #[test]
    fn each_malformed_token_gets_a_stand_in_of_its_kind() {
        let tokens = [
            ("1.5.5", true),
            ("1..5", true),
            ("1e", true),
            ("1e+", true),
            ("-1e", true),
            ("1ee5", true),
            ("1e5e5", true),
            ("1.5-", true),
            ("1e400.5", true),
            ("-.5", true),
            ("1.", true),
            ("01", false),
            ("00", false),
            ("-", false),
            ("--1", false),
            ("1-2", false),
        ];
        let mut seen = 0;
        for (token, decimal) in tokens {
            let body = format!(r#"{{"A":{token},"B":[{token}]}}"#);
            let (out, stand_ins) = replaced(&body);
            let value: serde_json::Value = serde_json::from_str(&out).expect(&out);
            assert_eq!(stand_ins.entries.len(), 2, "{token}");
            for number in [&value["A"], &value["B"][0]] {
                let number = number.as_number().expect("a number");
                assert_eq!(number.is_f64(), decimal, "{token}");
                assert_eq!(stand_ins.token(number), Some(token));
                seen += 1;
            }
            assert_ne!(value["A"], value["B"][0], "{token}: stand-ins are unique");
        }
        assert_eq!(seen, tokens.len() * 2);
    }

    #[test]
    fn strings_and_keys_are_left_alone() {
        let (out, stand_ins) = replaced(r#"{"1.5.5":"1.5.5","K":"a\"1.5.5","N":1.5.5}"#);
        assert!(out.starts_with(r#"{"1.5.5":"1.5.5","K":"a\"1.5.5","N":-"#));
        assert_eq!(stand_ins.entries.len(), 1);
    }

    #[test]
    fn a_stand_in_never_matches_a_number_or_text_in_the_body() {
        // The first candidate's digits are in a string, the second's value
        // is a number the client sent, so the third is chosen.
        let body = r#"{"S":"7310000001","N":-7310000002,"A":01}"#;
        let (out, _) = replaced(body);
        assert!(out.ends_with(r#""A":-7310000003}"#), "{out}");
        // Inside a longer run of digits, in a key or a number, too.
        let body = r#"{"x17310000001":1,"N":973100000029,"A":01}"#;
        let (out, _) = replaced(body);
        assert!(out.ends_with(r#""A":-7310000003}"#), "{out}");
        let body = r#"{"N":-7310000001.5,"M":-7.3100000025e9,"A":1.5.5}"#;
        let (out, _) = replaced(body);
        assert!(out.contains(r#""A":-7310000003.5"#), "{out}");
    }

    #[test]
    fn a_token_longer_than_its_stand_in_keeps_the_columns_after_it() {
        let body = r#"{"A":-.50000000000000000,"B":true}"#;
        let (out, _) = replaced(body);
        assert_eq!(out.len(), body.len());
        assert_eq!(out.find(r#""B""#), body.find(r#""B""#));
        assert!(serde_json::from_str::<serde_json::Value>(&out).is_ok());
    }

    #[test]
    fn the_twin_follows_dynamodbs_integer_rule() {
        for (token, twin_text) in [
            ("-", Some("0")),
            ("01", Some("1")),
            ("00", Some("0")),
            ("-00", Some("-0")),
            ("-.5", Some("-0.5")),
            ("1.", Some("1.0")),
            ("1.e5", Some("1.0e5")),
            ("01.50E+2", Some("1.50E+2")),
            ("1.5.5", None),
            ("1..5", None),
            ("1e", None),
            ("1e+", None),
            ("-1e", None),
            ("--1", None),
            ("1-2", None),
            ("1ee5", None),
            ("1e5e5", None),
            ("1.5-", None),
            ("1e400.5", None),
            ("-e5", None),
            ("-.", None),
            ("-.e5", None),
            ("-.e", None),
            ("-e", None),
        ] {
            assert_eq!(twin(token).as_deref(), twin_text, "{token}");
            if let Some(text) = twin_text {
                assert!(crate::serde_errors::json_number(text), "{text}");
            }
        }
    }

    #[test]
    fn integer_members_read_the_token_behind_a_stand_in() {
        let (out, stand_ins) = replaced(r#"{"A":01,"B":1.5.5,"C":-.5}"#);
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        let stand_ins = Rc::new(stand_ins);
        let read = |key: &str| {
            with_table(Some(stand_ins.clone()), || {
                read_integer(&value[key].to_string())
            })
        };
        assert!(matches!(
            read("A"),
            Ok(Integer {
                value: 1,
                lenient: true
            })
        ));
        assert!(matches!(read("B"), Err(IntegerFault::BadNumber)));
        assert!(matches!(
            read("C"),
            Ok(Integer {
                value: 0,
                lenient: true
            })
        ));
        // Outside the request the stand-in is an ordinary number.
        assert!(matches!(
            read_integer(&value["A"].to_string()),
            Ok(Integer {
                value: -7_310_000_001,
                lenient: false
            })
        ));
        assert!(matches!(
            read_integer("1"),
            Ok(Integer {
                value: 1,
                lenient: false
            })
        ));
    }

    #[test]
    fn a_stand_in_avoids_digits_a_string_holds_once_its_escapes_are_read() {
        // The string ends in the escape for `1`, so it reads as 7310000001
        // although the body never writes those ten digits in a row.
        let body = format!(r#"{{"T":"731000000{}u0031","A":01}}"#, '\\');
        assert!(!body.contains("7310000001"));
        let (out, _) = replaced(&body);
        assert!(out.ends_with(r#""A":-7310000002}"#), "{out}");
    }

    #[test]
    fn an_escaped_quote_does_not_end_the_string_whose_escapes_are_read() {
        // The first string holds an escaped quote before the escaped digits,
        // and the second string's escapes must still be read after it.
        let b = '\\';
        let body =
            format!(r#"{{"T":"a{b}"731000000{b}u0031","U":"{b}"731000000{b}u0032","A":01}}"#);
        let (out, _) = replaced(&body);
        assert!(out.ends_with(r#""A":-7310000003}"#), "{out}");
    }

    #[test]
    fn a_body_holding_many_candidates_still_gets_unique_absent_stand_ins() {
        let taken = 3000;
        let strings: Vec<String> = (0..taken)
            .map(|n| format!("\"{}\"", FIRST_STAND_IN + n))
            .collect();
        let tokens = vec!["01"; taken as usize];
        let body = format!(
            r#"{{"S":[{}],"A":[{}]}}"#,
            strings.join(","),
            tokens.join(",")
        );
        let (out, stand_ins) = replaced(&body);
        assert!(serde_json::from_str::<serde_json::Value>(&out).is_ok());
        assert_eq!(stand_ins.entries.len(), tokens.len());
        assert_eq!(stand_ins.entries[0].digits, FIRST_STAND_IN + taken);
        assert!(
            stand_ins
                .entries
                .windows(2)
                .all(|pair| pair[0].digits < pair[1].digits)
        );
    }

    #[test]
    fn only_an_error_answers_messages_are_read_for_a_stand_in() {
        let (_, stand_ins) = replaced(r#"{"A":01}"#);
        let digits = stand_ins.entries[0].digits;
        with_stand_ins(Some(stand_ins), || {
            // A stored item holding the digits is the item, not a stand-in.
            let item = format!(
                r#"{{"__type":"x#ConditionalCheckFailedException","message":"The conditional request failed","Item":{{"v":{{"S":"0{digits}"}}}}}}"#
            );
            assert!(!error_quotes_stand_in(&item));
            for quoting in [
                format!(r#"{{"__type":"x","message":"Value '-{digits}' at 'limit'"}}"#),
                format!(r#"{{"__type":"x","Message":"Value '-{digits}' at 'limit'"}}"#),
                format!(
                    r#"{{"__type":"x","Message":"Transaction cancelled","CancellationReasons":[{{"Code":"None"}},{{"Code":"ValidationError","Message":"Value '-{digits}'"}}]}}"#
                ),
            ] {
                assert!(error_quotes_stand_in(&quoting), "{quoting}");
            }
        });
    }

    #[test]
    fn each_request_keeps_its_own_table_across_interleaved_polls() {
        use std::future::Future;
        use std::pin::pin;
        use std::task::{Context, Poll, Waker};

        /// Pending once, then ready, so the table has to survive an await.
        struct YieldOnce(bool);
        impl Future for YieldOnce {
            type Output = ();
            fn poll(mut self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                if std::mem::replace(&mut self.0, true) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }

        let (out, stand_ins) = replaced(r#"{"A":01}"#);
        let quoted = format!("Value '{out}'");
        let mut with = pin!(with_stand_ins_async(Some(stand_ins), async {
            YieldOnce(false).await;
            quotes_stand_in(&quoted)
        }));
        let mut without = pin!(with_stand_ins_async(None, async {
            YieldOnce(false).await;
            quotes_stand_in(&quoted)
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(with.as_mut().poll(&mut cx).is_pending());
        assert!(without.as_mut().poll(&mut cx).is_pending());
        assert!(!quotes_stand_in(&quoted), "nothing between polls");
        assert_eq!(without.as_mut().poll(&mut cx), Poll::Ready(false));
        assert_eq!(with.as_mut().poll(&mut cx), Poll::Ready(true));
        assert!(!quotes_stand_in(&quoted));
    }

    #[test]
    fn the_table_is_cleared_when_a_request_panics() {
        let (out, stand_ins) = replaced(r#"{"A":01}"#);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_stand_ins(Some(stand_ins), || {
                assert!(quotes_stand_in(&out));
                panic!("the request failed");
            })
        }));
        assert!(result.is_err());
        assert!(!quotes_stand_in(&out));
    }

    #[test]
    fn an_answer_quoting_a_stand_in_is_caught_only_inside_its_request() {
        let (out, stand_ins) = replaced(r#"{"A":01}"#);
        let digits = stand_ins.entries[0].digits;
        assert!(!quotes_stand_in(&out));
        with_stand_ins(Some(stand_ins), || {
            assert!(quotes_stand_in(&out));
            assert!(quotes_stand_in(&format!("Value '{digits}' at 'limit'")));
            assert!(quotes_stand_in(&format!("Value '9{digits}' at 'limit'")));
            assert!(!quotes_stand_in(&format!(
                "Value '{}' at 'limit'",
                digits + 1
            )));
            assert!(!quotes_stand_in("Value '1' at 'limit'"));
        });
    }
}
