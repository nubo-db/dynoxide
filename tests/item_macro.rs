use dynoxide::AttributeValue;

#[test]
fn item_macro_simple() {
    let item = dynoxide::item! {
        "pk" => "user#1",
        "age" => 30i64,
        "active" => true,
    };
    assert_eq!(item["pk"], AttributeValue::S("user#1".to_string()));
    assert_eq!(item["age"], AttributeValue::N("30".to_string()));
    assert_eq!(item["active"], AttributeValue::BOOL(true));
    assert_eq!(item.len(), 3);
}

#[test]
fn item_macro_empty() {
    let item = dynoxide::item! {};
    assert!(item.is_empty());
}

#[test]
fn item_macro_single_entry() {
    let item = dynoxide::item! {
        "pk" => "val",
    };
    assert_eq!(item.len(), 1);
    assert_eq!(item["pk"], AttributeValue::S("val".to_string()));
}

#[test]
fn item_macro_nested_map() {
    let item = dynoxide::item! {
        "pk" => "user#1",
        "metadata" => {
            "count" => 5i64,
            "source" => "web",
        },
    };
    assert_eq!(item.len(), 2);
    match &item["metadata"] {
        AttributeValue::M(m) => {
            assert_eq!(m["count"], AttributeValue::N("5".to_string()));
            assert_eq!(m["source"], AttributeValue::S("web".to_string()));
        }
        _ => panic!("expected M"),
    }
}

#[test]
fn item_macro_deeply_nested_map() {
    let item = dynoxide::item! {
        "pk" => "x",
        "a" => {
            "b" => {
                "c" => "deep",
            },
        },
    };
    match &item["a"] {
        AttributeValue::M(a) => match &a["b"] {
            AttributeValue::M(b) => {
                assert_eq!(b["c"], AttributeValue::S("deep".to_string()));
            }
            _ => panic!("expected nested M"),
        },
        _ => panic!("expected M"),
    }
}

#[test]
fn item_macro_list() {
    let item = dynoxide::item! {
        "pk" => "x",
        "tags" => [AttributeValue::S("admin".into()), AttributeValue::S("editor".into())],
    };
    match &item["tags"] {
        AttributeValue::L(l) => {
            assert_eq!(l.len(), 2);
            assert_eq!(l[0], AttributeValue::S("admin".to_string()));
            assert_eq!(l[1], AttributeValue::S("editor".to_string()));
        }
        _ => panic!("expected L"),
    }
}

#[test]
fn item_macro_empty_list() {
    let item = dynoxide::item! {
        "pk" => "x",
        "tags" => [],
    };
    match &item["tags"] {
        AttributeValue::L(l) => assert!(l.is_empty()),
        _ => panic!("expected L"),
    }
}

#[test]
fn item_macro_option_none_omitted() {
    let email: Option<&str> = None;
    let item = dynoxide::item! {
        "pk" => "user#1",
        "email" => email,
    };
    assert!(!item.contains_key("email"));
    assert!(item.contains_key("pk"));
    assert_eq!(item.len(), 1);
}

#[test]
fn item_macro_option_some_included() {
    let email: Option<&str> = Some("a@b.com");
    let item = dynoxide::item! {
        "pk" => "user#1",
        "email" => email,
    };
    assert_eq!(item["email"], AttributeValue::S("a@b.com".to_string()));
    assert_eq!(item.len(), 2);
}

#[test]
fn item_macro_with_variable() {
    let name = "Alice";
    let age = 30i64;
    let item = dynoxide::item! {
        "pk" => "user#1",
        "name" => name,
        "age" => age,
    };
    assert_eq!(item["name"], AttributeValue::S("Alice".to_string()));
    assert_eq!(item["age"], AttributeValue::N("30".to_string()));
}

#[test]
fn item_macro_binary() {
    let data: Vec<u8> = vec![0xff, 0x00];
    let item = dynoxide::item! {
        "pk" => "x",
        "data" => data,
    };
    assert_eq!(item["data"], AttributeValue::B(vec![0xff, 0x00]));
}

#[test]
fn item_macro_no_trailing_comma() {
    let item = dynoxide::item! {
        "pk" => "x",
        "sk" => "y"
    };
    assert_eq!(item.len(), 2);
}
