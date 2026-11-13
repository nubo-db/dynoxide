//! PartiQL parameters fail on the fault DynamoDB reaches first.
//!
//! DynamoDB walks PartiQL parameters depth first and refuses the first value
//! nested too deep or number it can't read, whichever it meets first (captured
//! eu-west-2 and us-east-1, 2026-10-09). It takes a list's elements, and a
//! number set's members, in written order, and a map's members in the order a
//! Java `HashMap` sized for them iterates (captured eu-west-2, 2026-11-09, on
//! 140 maps of 2 to 40 members). ExecuteTransaction walks every statement's
//! parameters as one list: a deep value in the first statement beats a bad
//! number in the second, and the other way round (captured eu-west-2,
//! 2026-11-09). BatchExecuteStatement answers each statement on its own.
//!
//! Here a bad number fails the decode before the depth is checked, and a map
//! loses its member order once parsed, so the order is read from the text.
//! Where both kinds are present, every fault of the kind that comes second is
//! swapped for a harmless value: a bad number for `{"N":"0"}`, a map or list
//! at the refused depth for a leaf. The checks that run afterwards then answer
//! with the first. Every other body is left exactly as written.

use std::collections::HashSet;

use serde::de::IgnoredAny;

use crate::validation::java_hash_map_order;

use super::{MAX_JSON_DEPTH, MAX_NESTING, Role, UNREADABLE_NESTING, child_role, string_end};

/// The body with its parameter faults put in DynamoDB's order, and whether it
/// is still too deep for serde, when it needed rewriting. `None` leaves the
/// body as it is.
pub(crate) fn in_walk_order(operation: &str, body: &str) -> Option<(String, bool)> {
    let rewritten = rewrite(operation, body)?;
    // The walk reads structure, not syntax, so a body serde can't read for
    // another reason, before or after the rewrite, keeps the answer it had.
    // Read as the entry points read a body. (`IgnoredAny` would skip serde's
    // depth limit.)
    if crate::serde_errors::body_value(body).is_err_and(|e| !super::is_depth_failure(&e)) {
        return None;
    }
    match crate::serde_errors::body_value(&rewritten) {
        Ok(_) => Some((rewritten, false)),
        Err(e) if super::is_depth_failure(&e) => Some((rewritten, true)),
        Err(_) => None,
    }
}

fn rewrite(operation: &str, body: &str) -> Option<String> {
    // Where the decoder reads `Parameters`: on the request itself, or on each
    // element of its statement list, which ExecuteTransaction reads as one.
    let layout = match operation {
        "ExecuteStatement" => Layout {
            statements: None,
            one_list: false,
        },
        "BatchExecuteStatement" => Layout {
            statements: Some(b"Statements"),
            one_list: false,
        },
        "ExecuteTransaction" => Layout {
            statements: Some(b"TransactStatements"),
            one_list: true,
        },
        _ => return None,
    };
    if !body.contains("\"Parameters\"") {
        return None;
    }
    let Faults { found, first } = parameter_faults(body, &layout)?;
    let mut swaps: Vec<&Found> = found
        .iter()
        .filter(|f| first[f.list] != Some(f.fault))
        .collect();
    if swaps.is_empty() {
        return None;
    }
    swaps.sort_by_key(|f| f.start);
    let mut rewritten = String::with_capacity(body.len());
    let mut copied = 0;
    for swap in swaps {
        // The walk refuses a value with more than one member, so a swapped
        // value never holds another. Skipped rather than trusted all the same.
        if swap.start < copied {
            continue;
        }
        // A swap must not remove text serde would have refused. A number's
        // value is shallow, so it can be read on its own.
        if swap.fault == Fault::Number
            && serde_json::from_str::<IgnoredAny>(&body[swap.start..swap.end]).is_err()
        {
            return None;
        }
        rewritten.push_str(&body[copied..swap.start]);
        rewritten.push_str(match swap.fault {
            Fault::Number => r#"{"N":"0"}"#,
            Fault::Deep => r#"{"S":"leaf"}"#,
        });
        copied = swap.end;
    }
    rewritten.push_str(&body[copied..]);
    Some(rewritten)
}

struct Layout {
    /// The request member holding the statement list, when there is one.
    statements: Option<&'static [u8]>,
    /// Whether every statement's parameters are read as one list.
    one_list: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// An `M` or `L` as deep as a parameter's nesting check refuses.
    Deep,
    /// An `N`, or an `NS` member, that a parameter can't hold.
    Number,
}

/// One fault in a list of parameters.
#[derive(Debug)]
struct Found {
    fault: Fault,
    /// Which list it is in: each `Parameters` array, or the whole request.
    list: usize,
    /// Byte range of the attribute value that holds it.
    start: usize,
    end: usize,
}

/// Every parameter fault in a body, and the kind DynamoDB's walk meets first
/// in each list.
struct Faults {
    found: Vec<Found>,
    first: Vec<Option<Fault>>,
}

/// One step down from a list or a map towards a fault, resolved once every
/// map on the way has closed.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// A list element, by where it opens: written order.
    At(usize),
    /// A map's member: which map, and its place in written order.
    Member { map: usize, index: usize },
}

/// One open container in [`parameter_faults`].
struct Frame<'a> {
    is_object: bool,
    role: Role,
    key: (usize, usize),
    expect_key: bool,
    /// The list of parameters this sits in, if any.
    list: Option<usize>,
    nesting: u32,
    /// An `NS` array: its elements are numbers.
    number_set: bool,
    /// Member names already read, for an object inside a parameter.
    names: HashSet<&'a [u8]>,
    /// For an attribute value: where it opens, and the first fault it holds
    /// directly.
    start: usize,
    fault: Option<Fault>,
    /// The last step down to this container from its list of parameters, in
    /// the walk's shared list of steps.
    path: Option<usize>,
    /// For a map's members inside a parameter: which map it is, and its
    /// member names in written order.
    map: Option<(usize, Vec<&'a str>)>,
}

/// Every parameter fault in a body, walked as [`super::scan`] walks it.
/// `None`, leaving the body to the decoder, when the body is malformed, a
/// parameter nests past where DynamoDB stops reading, or a parameter holds a
/// shape the walk can't order as the decoder reads it: an attribute value
/// with more than one member, a repeated member name (serde keeps the last),
/// or an escaped member name.
fn parameter_faults(body: &str, layout: &Layout) -> Option<Faults> {
    let bytes = body.as_bytes();
    let mut stack: Vec<Frame> = Vec::new();
    // Each fault, with the last step down to it.
    let mut found: Vec<(Found, Option<usize>)> = Vec::new();
    // Every step down, each with the step before it and its list, so a
    // container holds one index rather than its whole way down.
    let mut steps: Vec<(Option<usize>, Step, usize)> = Vec::new();
    // Each map's members, by their place in written order, ranked in the
    // order DynamoDB's walk takes them, filled in as each map closes.
    let mut ranks: Vec<Vec<usize>> = Vec::new();
    let mut lists = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b',' => {
                if let Some(top) = stack.last_mut() {
                    top.expect_key = top.is_object;
                }
                i += 1;
            }
            b'"' => {
                let end = string_end(bytes, i)?;
                let depth = stack.len();
                if let Some(top) = stack.last_mut() {
                    if top.is_object && top.expect_key {
                        top.key = (i + 1, end - 1);
                        top.expect_key = false;
                        // The request and each statement are checked too, so
                        // a repeated `Parameters` can't put another list in play.
                        let on_the_path = depth == 1 || (layout.statements.is_some() && depth == 3);
                        if top.list.is_some() || on_the_path {
                            let name = &bytes[i + 1..end - 1];
                            let more_than_one =
                                top.role == Role::Attribute && !top.names.is_empty();
                            if more_than_one || name.contains(&b'\\') || !top.names.insert(name) {
                                return None;
                            }
                            if let Some((_, members)) = &mut top.map {
                                // Unescaped, so the text is the name.
                                members.push(&body[i + 1..end - 1]);
                            }
                        }
                    } else if top.list.is_some()
                        && if top.is_object {
                            top.role == Role::Attribute && &bytes[top.key.0..top.key.1] == b"N"
                        } else {
                            top.number_set
                        }
                    {
                        let text: String = serde_json::from_str(&body[i..end]).ok()?;
                        if crate::types::validate_parameter_number(&text).is_err() {
                            // A set's member belongs to the attribute value
                            // holding the set.
                            let holder = if top.is_object { depth - 1 } else { depth - 2 };
                            stack[holder].fault.get_or_insert(Fault::Number);
                        }
                    }
                }
                i = end;
            }
            b @ (b'{' | b'[') => {
                // A value with no member name: malformed.
                if stack.len() >= MAX_JSON_DEPTH
                    || stack.last().is_some_and(|p| p.is_object && p.expect_key)
                {
                    return None;
                }
                let is_object = b == b'{';
                let parent = stack.last();
                let key = parent.map_or(&b""[..], |p| &bytes[p.key.0..p.key.1]);
                let mut role = child_role(parent.map(|p| (p.role, p.is_object)), key);
                let mut list = parent.and_then(|p| p.list);
                let mut nesting = parent.map_or(0, |p| p.nesting);
                let mut number_set = false;
                match role {
                    Role::ValueList
                        if !is_object
                            && key == b"Parameters"
                            && read_by_the_decoder(&stack, bytes, layout) =>
                    {
                        list = Some(if layout.one_list { 0 } else { lists });
                        lists += 1;
                    }
                    Role::Attribute if is_object => {
                        nesting = match parent.map(|p| p.role) {
                            Some(Role::MapMembers | Role::ListElements) => nesting + 1,
                            _ => 0,
                        };
                        if list.is_some() && nesting > UNREADABLE_NESTING {
                            return None;
                        }
                    }
                    Role::MapMembers if is_object => {}
                    Role::ListElements if !is_object => {}
                    Role::Attribute | Role::MapMembers | Role::ListElements => role = Role::Opaque,
                    Role::Opaque => {
                        number_set = !is_object
                            && parent.is_some_and(|p| p.role == Role::Attribute)
                            && key == b"NS";
                    }
                    _ => {}
                }
                // A map or list this deep is what a parameter's nesting check
                // refuses (`value_nesting`), empty or not.
                if list.is_some()
                    && nesting == MAX_NESTING
                    && matches!(role, Role::MapMembers | Role::ListElements)
                {
                    let holder = stack.last_mut().expect("an attribute value holds it");
                    holder.fault.get_or_insert(Fault::Deep);
                }
                let mut path = None;
                if let (Some(parent), Some(list)) = (stack.last(), list) {
                    path = parent.path;
                    let step = match &parent.map {
                        Some((map, members)) => Some(Step::Member {
                            map: *map,
                            index: members.len().checked_sub(1)?,
                        }),
                        None if !parent.is_object => Some(Step::At(i)),
                        None => None,
                    };
                    if let Some(step) = step {
                        steps.push((path, step, list));
                        path = Some(steps.len() - 1);
                    }
                }
                let map = (list.is_some() && role == Role::MapMembers && is_object).then(|| {
                    ranks.push(Vec::new());
                    (ranks.len() - 1, Vec::new())
                });
                stack.push(Frame {
                    is_object,
                    role,
                    key: (0, 0),
                    expect_key: is_object,
                    list,
                    nesting,
                    number_set,
                    names: HashSet::new(),
                    start: i,
                    fault: None,
                    path,
                    map,
                });
                i += 1;
            }
            b @ (b'}' | b']') => {
                let frame = stack.pop()?;
                if frame.is_object != (b == b'}') {
                    return None;
                }
                if let Some((map, members)) = frame.map {
                    ranks[map] = java_hash_map_order(&members)?;
                }
                if let (Some(fault), Some(list)) = (frame.fault, frame.list) {
                    let start = frame.start;
                    found.push((
                        Found {
                            fault,
                            list,
                            start,
                            end: i + 1,
                        },
                        frame.path,
                    ));
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    if !stack.is_empty() {
        return None;
    }
    // Each list's first fault, in the order the walk meets the values
    // holding them. A fault held at a step comes ahead of everything below
    // it; below, the step the walk takes first wins. Every step is made after
    // the step before it, so going backwards hands each step's first fault
    // up before its own is settled.
    let mut held: Vec<Option<Fault>> = vec![None; steps.len()];
    let mut first: Vec<Option<Fault>> = vec![None; lists];
    for (f, path) in &found {
        match path {
            // The first found keeps its place, as a sort would.
            Some(step) => _ = held[*step].get_or_insert(f.fault),
            None => _ = first.get_mut(f.list)?.get_or_insert(f.fault),
        }
    }
    let mut below: Vec<Option<(usize, Fault)>> = vec![None; steps.len()];
    let mut top: Vec<Option<(usize, Fault)>> = vec![None; lists];
    for (step, &(before, kind, list)) in steps.iter().enumerate().rev() {
        let Some(fault) = held[step].or(below[step].map(|(_, fault)| fault)) else {
            continue;
        };
        let at = match kind {
            Step::At(at) => at,
            Step::Member { map, index } => *ranks.get(map)?.get(index)?,
        };
        let slot = match before {
            Some(before) => &mut below[before],
            None => top.get_mut(list)?,
        };
        if slot.is_none_or(|(best, _)| at < best) {
            *slot = Some((at, fault));
        }
    }
    for (first, top) in first.iter_mut().zip(top) {
        if first.is_none() {
            *first = top.map(|(_, fault)| fault);
        }
    }
    Some(Faults {
        found: found.into_iter().map(|(f, _)| f).collect(),
        first,
    })
}

/// Whether a `Parameters` array opening on top of `stack` is one the
/// operation's decoder reads: a member of the request, or of an element of
/// its statement list.
fn read_by_the_decoder(stack: &[Frame], bytes: &[u8], layout: &Layout) -> bool {
    match (layout.statements, stack) {
        (None, [_request]) => true,
        (Some(name), [request, list, _statement]) => {
            !list.is_object && &bytes[request.key.0..request.key.1] == name
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deep_map(depth: usize) -> String {
        deep_map_around(depth, r#"{"S":"leaf"}"#)
    }

    fn deep_map_around(depth: usize, leaf: &str) -> String {
        format!(
            "{}{leaf}{}",
            "{\"M\":{\"n\":".repeat(depth),
            "}}".repeat(depth)
        )
    }

    fn deep_list(depth: usize) -> String {
        format!(
            "{}{{\"S\":\"leaf\"}}{}",
            "{\"L\":[".repeat(depth),
            "]}".repeat(depth)
        )
    }

    /// An ExecuteStatement body with these parameters, written as given.
    fn statement(parameters: &str) -> String {
        format!(r#"{{"Statement":"SELECT * FROM t WHERE k = ?","Parameters":[{parameters}]}}"#)
    }

    fn rewritten(body: &str) -> Option<String> {
        rewrite("ExecuteStatement", body)
    }

    const BAD: &str = r#"{"N":"abc"}"#;
    const ZERO: &str = r#"{"N":"0"}"#;

    /// Which of `bad` a map with these member names, in written order, is
    /// walked to first.
    fn first_walked<'a>(names: &[&str], bad: &[&'a str]) -> &'a str {
        let ranks = java_hash_map_order(names).expect("no tree");
        bad.iter()
            .copied()
            .min_by_key(|b| ranks[names.iter().position(|n| n == b).unwrap()])
            .unwrap()
    }

    #[test]
    fn a_map_is_walked_in_java_hash_map_order() {
        // Each captured against eu-west-2 on 2026-11-09, as the bad number a
        // map holding several is refused for.
        assert_eq!(first_walked(&["a", "p"], &["a", "p"]), "p");
        assert_eq!(first_walked(&["p", "a"], &["p", "a"]), "p");
        assert_eq!(first_walked(&["b", "d"], &["b", "d"]), "d");
        // Names sharing a bucket keep written order.
        assert_eq!(first_walked(&["q", "a"], &["q", "a"]), "q");
        assert_eq!(first_walked(&["a", "q"], &["a", "q"]), "a");
        // Three names fill a table of four, six a table of eight.
        let three = ["8d_lho", "m6_t1", "abt"];
        assert_eq!(first_walked(&three, &three), "8d_lho");
        let six = ["vr7ybh", "wih", "j_c", "efg", "upr", "dxbfi"];
        assert_eq!(first_walked(&six, &six[..3]), "vr7ybh");
        // Hashed by UTF-16 unit, an emoji as two.
        let wide = [
            "üyy日éyxzb0本90",
            "ü0z9bay😀_ac_",
            "_z0😀xüb日zazz9",
            "_日_9日az0",
            "ba日ézxzé日z😀yxé本éazc",
            "b_本本za😀bxyéy本本0ü",
            "0byéübb0x😀yz",
            "zc",
        ];
        assert_eq!(
            first_walked(&wide, &[wide[2], wide[4], wide[5]]),
            "ba日ézxzé日z😀yxé本éazc"
        );
        // Hashing the UTF-8 bytes would put the third ahead here.
        let mixed = [
            "a0éb_czü日aéü本xé_",
            "😀",
            "0züü😀bc本b9ü本c0本9",
            "本b9z日日b😀b😀zü😀_",
            "日b0y",
        ];
        assert_eq!(
            first_walked(&mixed, &[mixed[0], mixed[3], mixed[4]]),
            "a0éb_czü日aéü本xé_"
        );
    }

    #[test]
    fn a_crowded_bucket_grows_the_table_until_it_would_become_a_tree() {
        // `Aa` and `BB` hash alike, so these sixteen names share one hash.
        let alike: Vec<String> = (0..16)
            .map(|n: u32| {
                (0..4)
                    .map(|bit| if n >> bit & 1 == 1 { "BB" } else { "Aa" })
                    .collect()
            })
            .collect();
        let mut names: Vec<&str> = alike.iter().map(String::as_str).take(9).collect();
        // Nine in one bucket of a small table: it doubles, and they keep
        // written order.
        assert_eq!(java_hash_map_order(&names), Some((0..9).collect()));
        // The doubling moves `x0` behind them, where a table of 16 would keep
        // it ahead (captured eu-west-2, 2026-11-09).
        let mut with_x0 = vec!["x0"];
        with_x0.extend(&names);
        assert_eq!(first_walked(&with_x0, &["x0", names[0]]), names[0]);
        // With 64 buckets or more, the ninth makes a tree.
        let others: Vec<String> = (0..60).map(|n| format!("k{n}")).collect();
        let mut many: Vec<&str> = others.iter().map(String::as_str).collect();
        many.append(&mut names);
        assert_eq!(java_hash_map_order(&many), None);
        // Thirty-three names start at 64 buckets, so the ninth alike makes a
        // tree straight away.
        let mut at_64: Vec<&str> = alike.iter().map(String::as_str).take(9).collect();
        at_64.extend(others.iter().map(String::as_str).take(24));
        assert_eq!(java_hash_map_order(&at_64), None);
        // Sixteen alike double the table from 16 to 32 to 64, then make a tree.
        let sixteen: Vec<&str> = alike.iter().map(String::as_str).collect();
        assert_eq!(java_hash_map_order(&sixteen), None);
    }

    #[test]
    fn each_map_is_walked_in_its_own_order() {
        let deep = deep_map(40);
        // The outer map is walked `p` first, so the inner map under `p`
        // decides; it is walked `q` first, as written.
        let body = format!(
            r#"{{"M":{{"a":{{"M":{{"n":{deep}}}}},"p":{{"M":{{"q":{BAD},"a":{deep}}}}}}}}}"#
        );
        assert_eq!(
            rewritten(&statement(&body)),
            Some(statement(&body.replace(&deep, &deep_map(30))))
        );
    }

    #[test]
    fn a_map_member_with_no_name_leaves_the_body_alone() {
        // Malformed, and answered by the decoder rather than the walk.
        for parameters in [
            format!(r#"{},{{"M":{{{BAD}}}}}"#, deep_map(100)),
            format!(r#"{{"M":{{{BAD}}}}}"#),
            format!(r#"{},{{"M":{{"a":{BAD},{BAD}}}}}"#, deep_map(40)),
            format!(r#"{},{{"M":{{["x"]}}}}"#, deep_map(100)),
        ] {
            let body = statement(&parameters);
            assert_eq!(rewritten(&body), None, "{parameters}");
            assert_eq!(in_walk_order("ExecuteStatement", &body), None);
        }
        // The same inside a batch or a transaction.
        let keyless = format!(r#"{},{{"M":{{{BAD}}}}}"#, deep_map(100));
        for (operation, key) in [
            ("BatchExecuteStatement", "Statements"),
            ("ExecuteTransaction", "TransactStatements"),
        ] {
            let body = format!(
                r#"{{"{key}":[{{"Statement":"SELECT * FROM t","Parameters":[{keyless}]}}]}}"#
            );
            assert_eq!(rewrite(operation, &body), None, "{operation}");
        }
    }

    #[test]
    fn a_fault_in_a_map_is_met_in_hash_map_order() {
        let deep = deep_map(40);
        // `p` comes ahead of `a`, wherever each is written.
        for body in [
            format!(r#"{{"M":{{"a":{deep},"p":{BAD}}}}}"#),
            format!(r#"{{"M":{{"p":{BAD},"a":{deep}}}}}"#),
        ] {
            assert_eq!(
                rewritten(&statement(&body)),
                Some(statement(&body.replace(&deep, &deep_map(31)))),
                "{}",
                &body[..20]
            );
        }
        // And `d` ahead of `b`.
        for body in [
            format!(r#"{{"M":{{"b":{BAD},"d":{deep}}}}}"#),
            format!(r#"{{"M":{{"d":{deep},"b":{BAD}}}}}"#),
        ] {
            assert_eq!(
                rewritten(&statement(&body)),
                Some(statement(&body.replace(BAD, ZERO))),
                "{}",
                &body[..20]
            );
        }
        // A map inside a list is ordered the same way.
        let body = format!(r#"{{"L":[{{"M":{{"a":{deep},"p":{BAD}}}}}]}}"#);
        assert_eq!(
            rewritten(&statement(&body)),
            Some(statement(&body.replace(&deep, &deep_map(30))))
        );
    }

    #[test]
    fn a_depth_written_first_swaps_the_numbers_after_it() {
        let deep = deep_map(40);
        assert_eq!(
            rewritten(&statement(&format!("{deep},{BAD}"))),
            Some(statement(&format!("{deep},{ZERO}")))
        );
        let overflow = r#"{"N":"1e126"}"#;
        assert_eq!(
            rewritten(&statement(&format!("{deep},{overflow}"))),
            Some(statement(&format!("{deep},{ZERO}")))
        );
        // Inside one map, `d` and `bad` share a bucket, so written order holds.
        assert_eq!(
            rewritten(&statement(&format!(
                r#"{{"M":{{"d":{deep},"bad":{BAD}}}}}"#
            ))),
            Some(statement(&format!(
                r#"{{"M":{{"d":{deep},"bad":{ZERO}}}}}"#
            )))
        );
        // A number below the depth comes after it.
        assert_eq!(
            rewritten(&statement(&deep_map_around(40, BAD))),
            Some(statement(&deep_map_around(40, ZERO)))
        );
        // A set member takes its whole set, so no duplicate is made.
        let list = deep_list(40);
        assert_eq!(
            rewritten(&statement(&format!(r#"{list},{{"NS":["1","abc"]}}"#))),
            Some(statement(&format!("{list},{ZERO}")))
        );
        // A set inside a map counts too.
        assert_eq!(
            rewritten(&statement(&format!(
                r#"{list},{{"M":{{"s":{{"NS":["abc"]}}}}}}"#
            ))),
            Some(statement(&format!(r#"{list},{{"M":{{"s":{ZERO}}}}}"#)))
        );
    }

    #[test]
    fn a_number_written_first_cuts_the_depths_after_it_to_the_limit() {
        // The map that would be the 33rd level becomes a leaf, so 32 remain.
        assert_eq!(
            rewritten(&statement(&format!("{BAD},{}", deep_map(40)))),
            Some(statement(&format!("{BAD},{}", deep_map(32))))
        );
        // One level down inside a map, 31 remain under the member.
        assert_eq!(
            rewritten(&statement(&format!(
                r#"{{"M":{{"bad":{BAD},"d":{}}}}}"#,
                deep_map(100)
            ))),
            Some(statement(&format!(
                r#"{{"M":{{"bad":{BAD},"d":{}}}}}"#,
                deep_map(31)
            )))
        );
        assert_eq!(
            rewritten(&statement(&format!(
                r#"{{"NS":["abc"]}},{}"#,
                deep_list(40)
            ))),
            Some(statement(&format!(r#"{{"NS":["abc"]}},{}"#, deep_list(32))))
        );
        // A number is read as the decoder reads it, escapes undone.
        let escaped = r#"{"N":"abc"}"#;
        assert_eq!(
            rewritten(&statement(&format!("{escaped},{}", deep_map(40)))),
            Some(statement(&format!("{escaped},{}", deep_map(32))))
        );
        let good = r#"{"N":"1"}"#;
        assert_eq!(
            rewritten(&statement(&format!("{good},{}", deep_map(40)))),
            None
        );
    }

    #[test]
    fn the_depth_counts_from_the_map_a_parameter_refuses() {
        // A leaf in 32 maps passes the parameter check; in 33 it doesn't.
        assert_eq!(
            rewritten(&statement(&format!("{},{BAD}", deep_map(32)))),
            None
        );
        assert!(rewritten(&statement(&format!("{},{BAD}", deep_map(33)))).is_some());
        // An empty map at that depth is refused all the same.
        let empty = deep_map_around(32, r#"{"M":{}}"#);
        assert_eq!(
            rewritten(&statement(&format!("{empty},{BAD}"))),
            Some(statement(&format!("{empty},{ZERO}")))
        );
    }

    #[test]
    fn a_statement_with_one_kind_of_fault_is_left_as_written() {
        for parameters in [
            deep_map(40),
            BAD.to_string(),
            format!("{BAD},{BAD}"),
            format!(r#"{{"N":"1"}},{}"#, deep_map(100)),
            r#"{"S":"abc"},{"N":"5"}"#.to_string(),
            // Only `N` and `NS` hold numbers.
            format!(r#"{{"SS":["abc"]}},{}"#, deep_map(40)),
            format!(r#"{{"BS":["abc"]}},{}"#, deep_map(40)),
            format!(r#"{{"S":"abc"}},{}"#, deep_map(40)),
        ] {
            assert_eq!(rewritten(&statement(&parameters)), None, "{parameters}");
        }
        // Not a PartiQL operation, or past where DynamoDB stops reading.
        let both = statement(&format!("{},{BAD}", deep_map(40)));
        assert_eq!(rewrite("PutItem", &both), None);
        assert_eq!(
            rewritten(&statement(&format!("{},{BAD}", deep_map(2001)))),
            None
        );
    }

    #[test]
    fn a_repeated_member_name_leaves_the_body_alone() {
        // serde keeps the last of a repeated name, so the walk can't know
        // which value the decoder will read.
        let deep = deep_map(40);
        for parameters in [
            format!(r#"{{"M":{{"a":{deep}}},"M":{{}}}},{BAD}"#),
            format!(r#"{deep},{{"N":"abc","N":"1"}}"#),
            format!(r#"{{"M":{{"a":{deep},"a":{{"S":"x"}}}}}},{BAD}"#),
        ] {
            assert_eq!(rewritten(&statement(&parameters)), None, "{parameters}");
        }
    }

    #[test]
    fn a_value_with_more_than_one_member_or_an_escaped_name_leaves_the_body_alone() {
        let deep = deep_map(40);
        for parameters in [
            // Overlapping values: the bad number's value holds another.
            format!(r#"{deep},{{"N":"abc","M":{{"a":{BAD}}}}}"#),
            format!(r#"{{"M":{{"n":{}}},"N":"abc"}}"#, deep_map(39)),
            // A name the walk reads as written but serde reads unescaped.
            format!(r#"{{"M":{{"a":{deep},"\u0061":{{"S":"x"}}}}}},{BAD}"#),
            format!(r#"{{"\u004e":"abc"}},{deep}"#),
        ] {
            assert_eq!(rewritten(&statement(&parameters)), None, "{parameters}");
        }
    }

    #[test]
    fn only_the_parameters_the_decoder_reads_are_walked() {
        let deep = deep_map(40);
        let transaction = |extra: &str| {
            format!(
                r#"{{{extra}"TransactStatements":[{{"Statement":"UPDATE t SET x = ?","Parameters":[{BAD}]}}]}}"#
            )
        };
        assert_eq!(
            rewrite(
                "ExecuteTransaction",
                &transaction(&format!(r#""Foo":{{"Parameters":[{deep}]}},"#))
            ),
            None
        );
        assert_eq!(
            rewrite(
                "ExecuteStatement",
                &format!(
                    r#"{{"Statement":"SELECT * FROM t","Foo":{{"Parameters":[{deep},{BAD}]}}}}"#
                )
            ),
            None
        );
        // A batch's statement list read as a transaction's is not walked.
        let as_batch = format!(
            r#"{{"Statements":[{{"Statement":"SELECT * FROM t","Parameters":[{deep},{BAD}]}}]}}"#
        );
        assert_eq!(rewrite("ExecuteTransaction", &as_batch), None);
        assert!(rewrite("BatchExecuteStatement", &as_batch).is_some());
    }

    #[test]
    fn a_repeated_request_or_statement_member_leaves_the_body_alone() {
        let both = format!("{},{BAD}", deep_map(40));
        for body in [
            format!(r#"{{"Statement":"SELECT * FROM t","Parameters":[{both}],"Parameters":[]}}"#),
            format!(
                r#"{{"Statement":"SELECT * FROM t","Parameters":[{both}],"\u0050arameters":[]}}"#
            ),
        ] {
            assert_eq!(rewritten(&body), None, "{body}");
        }
        assert_eq!(
            rewrite(
                "BatchExecuteStatement",
                &format!(
                    r#"{{"Statements":[{{"Statement":"SELECT * FROM t","Parameters":[{both}],"Parameters":[]}}]}}"#
                )
            ),
            None
        );
        // A number serde holds only as written, elsewhere in the body, is no
        // reason to leave the body alone.
        let limit =
            format!(r#"{{"Statement":"SELECT * FROM t","Parameters":[{both}],"Limit":1e400}}"#);
        assert!(in_walk_order("ExecuteStatement", &limit).is_some());
        // Such a number doesn't hide the depth either.
        let deep_limit = format!(
            r#"{{"Statement":"SELECT * FROM t","Limit":1e400,"Parameters":[{},{BAD}]}}"#,
            deep_map(100)
        );
        assert!(
            in_walk_order("ExecuteStatement", &deep_limit).is_some_and(|(_, too_deep)| too_deep)
        );
        // A statement's other members are no repeat.
        let batch = format!(
            r#"{{"Statements":[{{"Statement":"SELECT * FROM t","ConsistentRead":true,"Parameters":[{both}]}}],"ReturnConsumedCapacity":"NONE"}}"#
        );
        assert!(in_walk_order("BatchExecuteStatement", &batch).is_some());
    }

    #[test]
    fn a_rewrite_serde_cannot_read_is_dropped() {
        let deep = deep_map(100);
        let whole = statement(&format!("{BAD},{deep}"));
        let (rewritten, too_deep) = in_walk_order("ExecuteStatement", &whole).unwrap();
        assert!(!too_deep);
        assert_eq!(rewritten, statement(&format!("{BAD},{}", deep_map(32))));
        let deeper = in_walk_order("ExecuteStatement", &statement(&format!("{deep},{BAD}")));
        assert!(deeper.is_some_and(|(_, too_deep)| too_deep));
        // The walk doesn't check syntax, so a missing comma gets past it.
        let malformed = whole.replace(r#"}]}"#, r#"}] "Limit":5}"#);
        assert!(rewrite("ExecuteStatement", &malformed).is_some());
        assert_eq!(in_walk_order("ExecuteStatement", &malformed), None);
        // Nor can a swap remove text serde would refuse.
        let trailing_comma = statement(&format!(r#"{},{{"N":"abc",}}"#, deep_map(100)));
        assert_eq!(in_walk_order("ExecuteStatement", &trailing_comma), None);
        let shallow = statement(&format!(r#"{deep},{{"N":"abc",}}"#, deep = deep_map(40)));
        assert_eq!(in_walk_order("ExecuteStatement", &shallow), None);
    }

    #[test]
    fn batch_statements_keep_their_own_order_and_a_transaction_has_one() {
        let deep = deep_map(40);
        let two = |key: &str, first: &str, second: &str| {
            format!(
                r#"{{"{key}":[{{"Statement":"SELECT * FROM t","Parameters":[{first}]}},{{"Statement":"SELECT * FROM t","Parameters":[{second}]}}]}}"#
            )
        };
        let batch = |first: &str, second: &str| two("Statements", first, second);
        assert_eq!(
            rewrite(
                "BatchExecuteStatement",
                &batch(&format!("{deep},{BAD}"), &format!("{BAD},{deep}"))
            ),
            Some(batch(
                &format!("{deep},{ZERO}"),
                &format!("{BAD},{}", deep_map(32))
            ))
        );
        // A deep statement and a bad-number statement are each one kind.
        assert_eq!(rewrite("BatchExecuteStatement", &batch(&deep, BAD)), None);

        // A transaction reads all its statements' parameters as one list.
        let transaction = |first: &str, second: &str| two("TransactStatements", first, second);
        assert_eq!(
            rewrite("ExecuteTransaction", &transaction(&deep, BAD)),
            Some(transaction(&deep, ZERO))
        );
        assert_eq!(
            rewrite("ExecuteTransaction", &transaction(BAD, &deep)),
            Some(transaction(BAD, &deep_map(32)))
        );
    }
}
