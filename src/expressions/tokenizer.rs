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
        let c = bytes[i] as char;

        // Skip whitespace
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        match c {
            // Attribute name reference: #name
            '#' => {
                i += 1;
                let name_start = i;
                while i < bytes.len() && is_name_char(bytes[i] as char) {
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
                while i < bytes.len() && is_name_char(bytes[i] as char) {
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
                if i < bytes.len() && bytes[i] as char == '>' {
                    i += 1;
                    tokens.push((Token::Ne, TokenSpan::new(start, 2)));
                } else if i < bytes.len() && bytes[i] as char == '=' {
                    i += 1;
                    tokens.push((Token::Le, TokenSpan::new(start, 2)));
                } else {
                    tokens.push((Token::Lt, TokenSpan::new(start, 1)));
                }
            }

            '>' => {
                i += 1;
                if i < bytes.len() && bytes[i] as char == '=' {
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
                // Try to read a number inside brackets
                let num_start = i;
                while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                    i += 1;
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
                while i < bytes.len() && is_name_char(bytes[i] as char) {
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
    c.is_ascii_alphabetic() || c == '_'
}

fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
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

    /// Span of the token currently at `pos` (the next call to `peek`/`next` would return it).
    pub fn peek_span(&self) -> Option<TokenSpan> {
        self.tokens.get(self.pos).map(|(_, s)| *s)
    }

    /// Span of the token most recently returned by `next` (i.e. at `pos - 1`).
    pub fn current_span(&self) -> Option<TokenSpan> {
        if self.pos == 0 {
            None
        } else {
            self.tokens.get(self.pos - 1).map(|(_, s)| *s)
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.pos).map(|(t, _)| t);
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    pub fn expect(&mut self, expected: &Token) -> Result<(), String> {
        match self.next() {
            Some(t) if t == expected => Ok(()),
            Some(t) => Err(format!("Expected {expected}, got {t}")),
            None => Err(format!("Expected {expected}, got end of expression")),
        }
    }

    pub fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    /// Get the current position (alias for `position`).
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Set the stream position (used for backtracking).
    pub fn set_pos(&mut self, pos: usize) {
        self.pos = pos;
    }
}

/// Build the AWS-style `near: "..."` window for a parser-level syntax error
/// (such as UpdateExpression's "unexpected leading token"), where the offending
/// token has a known span and the window extends to include the next token if
/// one is present. The returned window is the original source slice from the
/// offending token's start to the next token's end (or the offending token's
/// end if no following token exists).
pub fn near_window_parser(source: &str, offending: TokenSpan, next: Option<TokenSpan>) -> &str {
    let end = match next {
        Some(span) => span.end(),
        None => offending.end(),
    };
    let end = end.min(source.len());
    &source[offending.start..end]
}

/// Build the AWS-style `near: "..."` window for a tokenizer-level error (such
/// as ProjectionExpression's stray `!`), where the failure point is a single
/// byte position in the source. The window captures the offending byte plus
/// at most one more contiguous non-whitespace byte, capped at the original
/// AWS-observed shape (e.g. `!!!` -> `!!`).
pub fn near_window_tokenizer(source: &str, position: usize) -> &str {
    let bytes = source.as_bytes();
    let mut end = position + 1;
    if end <= bytes.len() && end < bytes.len() && !(bytes[end] as char).is_whitespace() {
        end += 1;
    }
    let end = end.min(bytes.len());
    &source[position..end]
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

    #[test]
    fn test_near_window_parser_uses_next_token() {
        let source = "INVALID SYNTAX HERE";
        let offending = TokenSpan::new(0, 7);
        let next = Some(TokenSpan::new(8, 6));
        assert_eq!(
            near_window_parser(source, offending, next),
            "INVALID SYNTAX"
        );
    }

    #[test]
    fn test_near_window_parser_falls_back_to_offending_when_no_next() {
        let source = "BARE";
        let offending = TokenSpan::new(0, 4);
        assert_eq!(near_window_parser(source, offending, None), "BARE");
    }

    #[test]
    fn test_near_window_tokenizer_extends_one_char() {
        assert_eq!(near_window_tokenizer("!!! INVALID !!!", 0), "!!");
    }

    #[test]
    fn test_near_window_tokenizer_stops_at_whitespace() {
        // Only one offending char before whitespace; window stays single-char.
        assert_eq!(near_window_tokenizer("! foo", 0), "!");
    }
}
