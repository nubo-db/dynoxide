//! Shared tokenizer for all DynamoDB expression types.

use std::fmt;

/// Byte-offset span of a token (or tokenizer error) into the original source string.
/// Spans are byte offsets, not char indices; UpdateExpression and ProjectionExpression
/// callers slice the original source via `&source[span.start..span.start + span.len]`
/// to build the AWS-style `near: "..."` window in error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenSpan {
    pub start: usize,
    pub len: usize,
}

impl TokenSpan {
    pub fn new(start: usize, len: usize) -> Self {
        Self { start, len }
    }

    pub fn end(self) -> usize {
        self.start + self.len
    }
}

/// Structured tokenizer error carrying the byte position of the offending input,
/// so callers (UpdateExpression, ProjectionExpression) can build their own
/// `near: "..."` window from the original source rather than just a flat message.
#[derive(Debug, Clone)]
pub struct TokenizeError {
    pub message: String,
    pub position: usize,
    pub bad_len: usize,
}

impl fmt::Display for TokenizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Identifiers and references
    Identifier(String), // attribute name (e.g., `pk`, `myAttr`)
    NameRef(String),    // #name reference
    ValueRef(String),   // :value reference

    // Operators
    Eq,    // =
    Ne,    // <>
    Lt,    // <
    Le,    // <=
    Gt,    // >
    Ge,    // >=
    Plus,  // +
    Minus, // -

    // Keywords (case-insensitive)
    And,
    Or,
    Not,
    Between,
    In,
    Set,
    Remove,
    Add,
    Delete,

    // Punctuation
    LParen,   // (
    RParen,   // )
    LBracket, // [
    RBracket, // ]
    Dot,      // .
    Comma,    // ,

    // Literals
    Number(String), // numeric literal in brackets [0], [1]
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Identifier(s) => write!(f, "{s}"),
            Token::NameRef(s) => write!(f, "{s}"),
            Token::ValueRef(s) => write!(f, "{s}"),
            Token::Eq => write!(f, "="),
            Token::Ne => write!(f, "<>"),
            Token::Lt => write!(f, "<"),
            Token::Le => write!(f, "<="),
            Token::Gt => write!(f, ">"),
            Token::Ge => write!(f, ">="),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Not => write!(f, "NOT"),
            Token::Between => write!(f, "BETWEEN"),
            Token::In => write!(f, "IN"),
            Token::Set => write!(f, "SET"),
            Token::Remove => write!(f, "REMOVE"),
            Token::Add => write!(f, "ADD"),
            Token::Delete => write!(f, "DELETE"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::Dot => write!(f, "."),
            Token::Comma => write!(f, ","),
            Token::Number(n) => write!(f, "{n}"),
        }
    }
}

/// Tokenize a DynamoDB expression string, returning each token together with its
/// byte-offset span into the original input. Callers use the spans to build
/// `near: "..."` windows in syntax error messages.
pub fn tokenize(input: &str) -> Result<Vec<(Token, TokenSpan)>, TokenizeError> {
    let mut tokens: Vec<(Token, TokenSpan)> = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let start = i;
        // `i` is always on a character boundary: every arm below steps over
        // ASCII bytes or a whole character.
        let c = input[i..].chars().next().unwrap_or_default();

        // Skip ASCII whitespace. DynamoDB reads any other character outside a
        // string, a no-break space included, as a token it can't read
        // (eu-west-2 and us-east-1, 2026-10-09).
        if c.is_ascii() && c.is_whitespace() {
            i += 1;
            continue;
        }

        match c {
            // Attribute name reference: #name
            '#' => {
                i += 1;
                let name_start = i;
                while i < bytes.len() && is_name_byte(bytes[i]) {
                    i += 1;
                }
                if i == name_start {
                    return Err(TokenizeError {
                        message: "Syntax error; token: \"#\"".to_string(),
                        position: start,
                        bad_len: 1,
                    });
                }
                let name = &input[name_start..i];
                tokens.push((
                    Token::NameRef(format!("#{name}")),
                    TokenSpan::new(start, i - start),
                ));
            }

            // Attribute value reference: :value
            ':' => {
                i += 1;
                let name_start = i;
                while i < bytes.len() && is_name_byte(bytes[i]) {
                    i += 1;
                }
                if i == name_start {
                    return Err(TokenizeError {
                        message: "Syntax error; token: \":\"".to_string(),
                        position: start,
                        bad_len: 1,
                    });
                }
                let name = &input[name_start..i];
                tokens.push((
                    Token::ValueRef(format!(":{name}")),
                    TokenSpan::new(start, i - start),
                ));
            }

            // Comparison operators
            '<' => {
                i += 1;
                if i < bytes.len() && bytes[i] == b'>' {
                    i += 1;
                    tokens.push((Token::Ne, TokenSpan::new(start, 2)));
                } else if i < bytes.len() && bytes[i] == b'=' {
                    i += 1;
                    tokens.push((Token::Le, TokenSpan::new(start, 2)));
                } else {
                    tokens.push((Token::Lt, TokenSpan::new(start, 1)));
                }
            }

            '>' => {
                i += 1;
                if i < bytes.len() && bytes[i] == b'=' {
                    i += 1;
                    tokens.push((Token::Ge, TokenSpan::new(start, 2)));
                } else {
                    tokens.push((Token::Gt, TokenSpan::new(start, 1)));
                }
            }

            '=' => {
                i += 1;
                tokens.push((Token::Eq, TokenSpan::new(start, 1)));
            }
            '+' => {
                i += 1;
                tokens.push((Token::Plus, TokenSpan::new(start, 1)));
            }
            '-' => {
                i += 1;
                tokens.push((Token::Minus, TokenSpan::new(start, 1)));
            }

            // Punctuation
            '(' => {
                i += 1;
                tokens.push((Token::LParen, TokenSpan::new(start, 1)));
            }
            ')' => {
                i += 1;
                tokens.push((Token::RParen, TokenSpan::new(start, 1)));
            }
            '[' => {
                i += 1;
                tokens.push((Token::LBracket, TokenSpan::new(start, 1)));
                // Try to read a number inside brackets. A number starting with
                // 0 is the 0 alone, as DynamoDB reads it, so `l[00]` leaves a
                // second digit the tokenizer then refuses (`token: "0", near:
                // "00]"`, eu-west-2, 2026-10-09).
                let num_start = i;
                if i < bytes.len() && bytes[i] == b'0' {
                    i += 1;
                } else {
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                if i > num_start {
                    let num = input[num_start..i].to_string();
                    tokens.push((Token::Number(num), TokenSpan::new(num_start, i - num_start)));
                }
            }
            ']' => {
                i += 1;
                tokens.push((Token::RBracket, TokenSpan::new(start, 1)));
            }
            '.' => {
                i += 1;
                tokens.push((Token::Dot, TokenSpan::new(start, 1)));
            }
            ',' => {
                i += 1;
                tokens.push((Token::Comma, TokenSpan::new(start, 1)));
            }

            // Identifiers and keywords
            c if is_ident_start(c) => {
                let ident_start = i;
                while i < bytes.len() && is_name_byte(bytes[i]) {
                    i += 1;
                }
                let word = &input[ident_start..i];
                let token = match word.to_uppercase().as_str() {
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    "BETWEEN" => Token::Between,
                    "IN" => Token::In,
                    "SET" => Token::Set,
                    "REMOVE" => Token::Remove,
                    "ADD" => Token::Add,
                    "DELETE" => Token::Delete,
                    _ => Token::Identifier(word.to_string()),
                };
                tokens.push((token, TokenSpan::new(ident_start, i - ident_start)));
            }

            c => {
                return Err(TokenizeError {
                    message: format!("Syntax error; token: \"{c}\""),
                    position: start,
                    bad_len: c.len_utf8(),
                });
            }
        }
    }

    Ok(tokens)
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic()
}

fn is_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// A cursor over a token stream for parsing.
pub struct TokenStream {
    tokens: Vec<(Token, TokenSpan)>,
    pos: usize,
}

impl TokenStream {
    pub fn new(tokens: Vec<(Token, TokenSpan)>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|(t, _)| t)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.pos).map(|(t, _)| t);
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    /// Consume the next token if it is `expected`. On a mismatch the token is
    /// left in place, so the stream still points at it when the caller turns
    /// the failure into a `token: ..., near: ...` message.
    pub fn expect(&mut self, expected: &Token) -> Result<(), String> {
        match self.peek() {
            Some(t) if t == expected => {
                self.pos += 1;
                Ok(())
            }
            Some(t) => Err(format!("Expected {expected}, got {t}")),
            None => Err(format!("Expected {expected}, got end of expression")),
        }
    }

    /// The token `offset` places after the current one, without consuming.
    pub fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset).map(|(t, _)| t)
    }

    /// DynamoDB's syntax error for the token the stream is stopped at, or for
    /// the end of the expression when it has run out. See [`syntax_error_at`].
    pub fn syntax_error(&self, source: &str) -> String {
        let spans: Vec<TokenSpan> = self.tokens.iter().map(|(_, s)| *s).collect();
        syntax_error_at(source, &spans, self.pos)
    }

    pub fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    /// The index of the token the next call to `peek` or `next` returns.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Set the stream position (used for backtracking).
    pub fn set_pos(&mut self, pos: usize) {
        self.pos = pos;
    }
}

/// DynamoDB's syntax error naming the token at `index` in `spans` (or the end
/// of the expression when `index` is past the last token):
/// `Syntax error; token: "x", near: ") x y"`.
///
/// `near` is the source from the start of the token before the offending one
/// to the end of the token after it, so it holds up to three tokens with their
/// original spacing. At the end of the expression the token is `<EOF>` and
/// `near` is the last token read. Captured in us-east-1 and eu-west-2,
/// 2026-10-09, across ConditionExpression, FilterExpression,
/// KeyConditionExpression and UpdateExpression.
///
/// An expression with no tokens at all, only whitespace, is the same error at
/// its end with the whitespace as sent for `near`: `near: "   "`, or a tab and
/// a newline as they were (captured eu-west-2 and us-east-1, 2026-10-10).
pub fn syntax_error_at(source: &str, spans: &[TokenSpan], index: usize) -> String {
    let Some(offending) = spans.get(index) else {
        let near = spans
            .last()
            .map_or(source, |last| &source[last.start..last.end()]);
        return format!(r#"Syntax error; token: "<EOF>", near: "{near}""#);
    };
    let start = if index > 0 {
        spans[index - 1].start
    } else {
        offending.start
    };
    let end = spans
        .get(index + 1)
        .map_or(offending.end(), |next| next.end());
    format!(
        r#"Syntax error; token: "{}", near: "{}""#,
        &source[offending.start..offending.end()],
        &source[start..end]
    )
}

/// DynamoDB's syntax error for a character the tokenizer can't read, such as a
/// stray `!`. The character counts as a token of its own, so `near` takes in
/// the token before it and the next token or unreadable character after it.
pub fn tokenizer_syntax_error(source: &str, err: &TokenizeError) -> String {
    let bad = TokenSpan::new(err.position, err.bad_len);
    // Everything before the first unreadable character tokenizes.
    let mut spans: Vec<TokenSpan> = tokenize(&source[..err.position])
        .map(|tokens| tokens.into_iter().map(|(_, s)| s).collect())
        .unwrap_or_default();
    let index = spans.len();
    spans.push(bad);
    if let Some(next) = next_lexeme(source, bad.end()) {
        spans.push(next);
    }
    syntax_error_at(source, &spans, index)
}

/// The span of the first token, or unreadable character, at or after `from`.
fn next_lexeme(source: &str, from: usize) -> Option<TokenSpan> {
    let rest = &source[from..];
    let skipped = rest.trim_start_matches(|c: char| c.is_ascii() && c.is_whitespace());
    let start = from + (rest.len() - skipped.len());
    if start >= source.len() {
        return None;
    }
    let offset = |s: TokenSpan| TokenSpan::new(start + s.start, s.len);
    match tokenize(&source[start..]) {
        Ok(tokens) => tokens.first().map(|(_, s)| offset(*s)),
        Err(e) if e.position == 0 => Some(TokenSpan::new(start, e.bad_len)),
        Err(e) => tokenize(&source[start..start + e.position])
            .ok()
            .and_then(|tokens| tokens.first().map(|(_, s)| offset(*s))),
    }
}

/// Detect redundant parentheses, matching DynamoDB's rule: a parenthesised
/// group whose entire content is itself a single parenthesised group, e.g.
/// `((expr))` or `((a)) AND (b)`. A single wrap like `(expr)` is fine, and a
/// wrap around a real combination like `((a) AND (b))` is fine. Shared by
/// condition/filter and key-condition parsing. Returns the raw reason; callers
/// add the `Invalid <X>Expression:` prefix.
pub fn check_redundant_parens(tokens: &[(Token, TokenSpan)]) -> Result<(), String> {
    let toks: Vec<&Token> = tokens.iter().map(|(t, _)| t).collect();
    let match_close = |open: usize| -> Option<usize> {
        let mut depth = 0;
        for (j, tok) in toks.iter().enumerate().skip(open) {
            match tok {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(j);
                    }
                }
                _ => {}
            }
        }
        None
    };
    for i in 0..toks.len() {
        if !matches!(toks[i], Token::LParen) {
            continue;
        }
        let Some(outer_close) = match_close(i) else {
            continue;
        };
        // Redundant when the outer group's entire content is a single
        // parenthesised group: the token after the outer `(` is itself `(`
        // and its match sits immediately before the outer `)`.
        if matches!(toks.get(i + 1), Some(Token::LParen))
            && let Some(inner_close) = match_close(i + 1)
            && inner_close == outer_close - 1
        {
            return Err("The expression has redundant parentheses;".to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn just_tokens(input: &str) -> Vec<Token> {
        tokenize(input)
            .unwrap()
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    #[test]
    fn test_tokenize_simple_condition() {
        assert_eq!(
            just_tokens("#status = :val"),
            vec![
                Token::NameRef("#status".into()),
                Token::Eq,
                Token::ValueRef(":val".into()),
            ]
        );
    }

    #[test]
    fn test_tokenize_comparison_operators() {
        let tokens = just_tokens("a < b");
        assert!(matches!(tokens[1], Token::Lt));

        let tokens = just_tokens("a <= b");
        assert!(matches!(tokens[1], Token::Le));

        let tokens = just_tokens("a > b");
        assert!(matches!(tokens[1], Token::Gt));

        let tokens = just_tokens("a >= b");
        assert!(matches!(tokens[1], Token::Ge));

        let tokens = just_tokens("a <> b");
        assert!(matches!(tokens[1], Token::Ne));
    }

    #[test]
    fn test_tokenize_keywords() {
        let tokens = just_tokens("a AND b OR NOT c BETWEEN d IN e");
        assert!(matches!(tokens[1], Token::And));
        assert!(matches!(tokens[3], Token::Or));
        assert!(matches!(tokens[4], Token::Not));
        assert!(matches!(tokens[6], Token::Between));
        assert!(matches!(tokens[8], Token::In));
    }

    #[test]
    fn test_tokenize_update_keywords() {
        let tokens = just_tokens("SET a = :v REMOVE b ADD c :d DELETE e :f");
        assert!(matches!(tokens[0], Token::Set));
        assert!(matches!(tokens[4], Token::Remove));
        assert!(matches!(tokens[6], Token::Add));
        assert!(matches!(tokens[9], Token::Delete));
    }

    #[test]
    fn test_tokenize_path_expression() {
        assert_eq!(
            just_tokens("a.b[0].c"),
            vec![
                Token::Identifier("a".into()),
                Token::Dot,
                Token::Identifier("b".into()),
                Token::LBracket,
                Token::Number("0".into()),
                Token::RBracket,
                Token::Dot,
                Token::Identifier("c".into()),
            ]
        );
    }

    #[test]
    fn test_tokenize_function_call() {
        assert_eq!(
            just_tokens("attribute_exists(#name)"),
            vec![
                Token::Identifier("attribute_exists".into()),
                Token::LParen,
                Token::NameRef("#name".into()),
                Token::RParen,
            ]
        );
    }

    #[test]
    fn test_tokenize_arithmetic() {
        let tokens = just_tokens("Price + :inc");
        assert!(matches!(tokens[1], Token::Plus));

        let tokens = just_tokens("Price - :dec");
        assert!(matches!(tokens[1], Token::Minus));
    }

    #[test]
    fn test_tokenize_case_insensitive_keywords() {
        let tokens = just_tokens("set AND or");
        assert!(matches!(tokens[0], Token::Set));
        assert!(matches!(tokens[1], Token::And));
        assert!(matches!(tokens[2], Token::Or));
    }

    #[test]
    fn test_tokenize_returns_byte_spans() {
        let tokens = tokenize("INVALID SYNTAX HERE").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].1, TokenSpan::new(0, 7));
        assert_eq!(tokens[1].1, TokenSpan::new(8, 6));
        assert_eq!(tokens[2].1, TokenSpan::new(15, 4));
    }

    #[test]
    fn test_tokenize_error_carries_position() {
        let err = tokenize("!!!").unwrap_err();
        assert_eq!(err.message, "Syntax error; token: \"!\"");
        assert_eq!(err.position, 0);
    }

    fn syntax_error_for(source: &str, index: usize) -> String {
        let spans: Vec<TokenSpan> = tokenize(source)
            .unwrap()
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        syntax_error_at(source, &spans, index)
    }

    #[test]
    fn test_syntax_error_near_takes_the_tokens_either_side() {
        assert_eq!(
            syntax_error_for("INVALID SYNTAX HERE", 0),
            r#"Syntax error; token: "INVALID", near: "INVALID SYNTAX""#
        );
        assert_eq!(
            syntax_error_for("attribute_exists(pk) x y", 4),
            r#"Syntax error; token: "x", near: ") x y""#
        );
        assert_eq!(
            syntax_error_for("a = :v)", 3),
            r#"Syntax error; token: ")", near: ":v)""#
        );
    }

    #[test]
    fn test_syntax_error_at_the_end_names_the_last_token() {
        assert_eq!(
            syntax_error_for("attribute_not_exists(", 2),
            r#"Syntax error; token: "<EOF>", near: "(""#
        );
    }

    #[test]
    fn test_tokenizer_syntax_error_treats_the_character_as_a_token() {
        let near = |source: &str| {
            let err = tokenize(source).unwrap_err();
            tokenizer_syntax_error(source, &err)
        };
        assert_eq!(near("!!!"), r#"Syntax error; token: "!", near: "!!""#);
        assert_eq!(
            near("a = :v !"),
            r#"Syntax error; token: "!", near: ":v !""#
        );
        assert_eq!(near("a ! b"), r#"Syntax error; token: "!", near: "a ! b""#);
    }

    /// A character outside ASCII is a token of its own, whatever its width,
    /// and so is a no-break space. Captured as ConditionExpressions in
    /// eu-west-2 and us-east-1, 2026-10-09.
    #[test]
    fn test_tokenizer_syntax_error_names_a_non_ascii_character() {
        let near = |source: &str| {
            let err = tokenize(source).unwrap_err();
            tokenizer_syntax_error(source, &err)
        };
        for c in ["é", "€", "\u{1F600}", "\u{a0}"] {
            for (source, window) in [
                (format!("{c}a = :v"), format!("{c}a")),
                (format!("a {c} = :v"), format!("a {c} =")),
                (format!("a = :v {c}"), format!(":v {c}")),
                (format!("a = :v{c}"), format!(":v{c}")),
                (format!("a{c} = :v"), format!("a{c} =")),
            ] {
                assert_eq!(
                    near(&source),
                    format!(r#"Syntax error; token: "{c}", near: "{window}""#),
                    "{source:?}"
                );
            }
        }
    }
}
