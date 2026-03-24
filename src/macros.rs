/// Trait for inserting values into an item map, with `Option<T>` support.
///
/// When the value is `None`, the key is omitted from the map.
/// This is not public API — it exists to support the `item!` macro.
#[doc(hidden)]
pub trait ItemInsert {
    fn __item_insert(
        self,
        map: &mut std::collections::HashMap<String, crate::AttributeValue>,
        key: &str,
    );
}

impl<T: Into<crate::AttributeValue>> ItemInsert for T {
    fn __item_insert(
        self,
        map: &mut std::collections::HashMap<String, crate::AttributeValue>,
        key: &str,
    ) {
        map.insert(key.to_string(), self.into());
    }
}

impl<T: Into<crate::AttributeValue>> ItemInsert for Option<T> {
    fn __item_insert(
        self,
        map: &mut std::collections::HashMap<String, crate::AttributeValue>,
        key: &str,
    ) {
        if let Some(v) = self {
            map.insert(key.to_string(), v.into());
        }
    }
}

/// Construct a `HashMap<String, AttributeValue>` from key-value pairs.
///
/// Values are converted via `Into<AttributeValue>`. `Option` values that are
/// `None` are omitted from the map (the key is not inserted).
///
/// # Examples
///
/// ```
/// use dynoxide::item;
/// use dynoxide::AttributeValue;
///
/// let item = item! {
///     "pk" => "user#1",
///     "age" => 30i64,
///     "active" => true,
/// };
/// assert_eq!(item["pk"], AttributeValue::S("user#1".to_string()));
/// assert_eq!(item["age"], AttributeValue::N("30".to_string()));
/// ```
///
/// Nested maps:
/// ```
/// use dynoxide::item;
///
/// let item = item! {
///     "pk" => "user#1",
///     "metadata" => {
///         "count" => 5i64,
///     },
/// };
/// ```
///
/// Option handling (None values are omitted):
/// ```
/// use dynoxide::item;
///
/// let email: Option<&str> = None;
/// let item = item! {
///     "pk" => "user#1",
///     "email" => email,
/// };
/// assert!(!item.contains_key("email"));
/// ```
#[macro_export]
macro_rules! item {
    // Entry point: empty
    () => {{
        ::std::collections::HashMap::<String, $crate::AttributeValue>::new()
    }};
    // Entry point: key-value pairs (delegates to internal muncher)
    ( $($rest:tt)+ ) => {{
        #[allow(unused_mut)]
        let mut map = ::std::collections::HashMap::<String, $crate::AttributeValue>::new();
        $crate::__item_internal!(map, $($rest)+);
        map
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __item_internal {
    // Nested map value, more pairs follow
    ($map:ident, $key:expr => { $($inner:tt)* }, $($rest:tt)+) => {
        $map.insert($key.to_string(), $crate::AttributeValue::M($crate::item! { $($inner)* }));
        $crate::__item_internal!($map, $($rest)+);
    };
    // Nested map value, last pair
    ($map:ident, $key:expr => { $($inner:tt)* } $(,)?) => {
        $map.insert($key.to_string(), $crate::AttributeValue::M($crate::item! { $($inner)* }));
    };
    // List value, more pairs follow
    ($map:ident, $key:expr => [ $($elem:expr),* $(,)? ], $($rest:tt)+) => {
        $map.insert($key.to_string(), $crate::AttributeValue::L(vec![$($elem),*]));
        $crate::__item_internal!($map, $($rest)+);
    };
    // List value, last pair
    ($map:ident, $key:expr => [ $($elem:expr),* $(,)? ] $(,)?) => {
        $map.insert($key.to_string(), $crate::AttributeValue::L(vec![$($elem),*]));
    };
    // Expression value, more pairs follow
    ($map:ident, $key:expr => $val:expr, $($rest:tt)+) => {
        $crate::ItemInsert::__item_insert($val, &mut $map, $key);
        $crate::__item_internal!($map, $($rest)+);
    };
    // Expression value, last pair
    ($map:ident, $key:expr => $val:expr $(,)?) => {
        $crate::ItemInsert::__item_insert($val, &mut $map, $key);
    };
}
