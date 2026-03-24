//! Shared tokenizer for all DynamoDB expression types.

use std::fmt;

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

/// Tokenize a DynamoDB expression string.
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Skip whitespace
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        match chars[i] {
            // Attribute name reference: #name
            '#' => {
                i += 1;
                let start = i;
                while i < chars.len() && is_name_char(chars[i]) {
                    i += 1;
                }
                if i == start {
                    return Err("Syntax error; token: \"#\"".to_string());
                }
                let name: String = chars[start..i].iter().collect();
                tokens.push(Token::NameRef(format!("#{name}")));
            }

            // Attribute value reference: :value
            ':' => {
                i += 1;
                let start = i;
                while i < chars.len() && is_name_char(chars[i]) {
                    i += 1;
                }
                if i == start {
                    return Err("Syntax error; token: \":\"".to_string());
                }
                let name: String = chars[start..i].iter().collect();
                tokens.push(Token::ValueRef(format!(":{name}")));
            }

            // Comparison operators
            '<' => {
                i += 1;
                if i < chars.len() && chars[i] == '>' {
                    tokens.push(Token::Ne);
                    i += 1;
                } else if i < chars.len() && chars[i] == '=' {
                    tokens.push(Token::Le);
                    i += 1;
                } else {
                    tokens.push(Token::Lt);
                }
            }

            '>' => {
                i += 1;
                if i < chars.len() && chars[i] == '=' {
                    tokens.push(Token::Ge);
                    i += 1;
                } else {
                    tokens.push(Token::Gt);
                }
            }

            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }

            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }

            // Punctuation
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                // Parse bracket with numeric index
                tokens.push(Token::LBracket);
                i += 1;
                // Try to read a number inside brackets
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                if i > start {
                    let num: String = chars[start..i].iter().collect();
                    tokens.push(Token::Number(num));
                }
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }

            // Identifiers and keywords
            c if is_ident_start(c) => {
                let start = i;
                while i < chars.len() && is_name_char(chars[i]) {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
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
                    _ => Token::Identifier(word),
                };
                tokens.push(token);
            }

            c => {
                return Err(format!("Syntax error; token: \"{c}\""));
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
    tokens: Vec<Token>,
    pos: usize,
}

impl TokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.pos);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple_condition() {
        let tokens = tokenize("#status = :val").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::NameRef("#status".into()),
                Token::Eq,
                Token::ValueRef(":val".into()),
            ]
        );
    }

    #[test]
    fn test_tokenize_comparison_operators() {
        let tokens = tokenize("a < b").unwrap();
        assert!(matches!(tokens[1], Token::Lt));

        let tokens = tokenize("a <= b").unwrap();
        assert!(matches!(tokens[1], Token::Le));

        let tokens = tokenize("a > b").unwrap();
        assert!(matches!(tokens[1], Token::Gt));

        let tokens = tokenize("a >= b").unwrap();
        assert!(matches!(tokens[1], Token::Ge));

        let tokens = tokenize("a <> b").unwrap();
        assert!(matches!(tokens[1], Token::Ne));
    }

    #[test]
    fn test_tokenize_keywords() {
        let tokens = tokenize("a AND b OR NOT c BETWEEN d IN e").unwrap();
        assert!(matches!(tokens[1], Token::And));
        assert!(matches!(tokens[3], Token::Or));
        assert!(matches!(tokens[4], Token::Not));
        assert!(matches!(tokens[6], Token::Between));
        assert!(matches!(tokens[8], Token::In));
    }

    #[test]
    fn test_tokenize_update_keywords() {
        let tokens = tokenize("SET a = :v REMOVE b ADD c :d DELETE e :f").unwrap();
        assert!(matches!(tokens[0], Token::Set));
        assert!(matches!(tokens[4], Token::Remove));
        assert!(matches!(tokens[6], Token::Add));
        assert!(matches!(tokens[9], Token::Delete));
    }

    #[test]
    fn test_tokenize_path_expression() {
        let tokens = tokenize("a.b[0].c").unwrap();
        assert_eq!(
            tokens,
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
        let tokens = tokenize("attribute_exists(#name)").unwrap();
        assert_eq!(
            tokens,
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
        let tokens = tokenize("Price + :inc").unwrap();
        assert!(matches!(tokens[1], Token::Plus));

        let tokens = tokenize("Price - :dec").unwrap();
        assert!(matches!(tokens[1], Token::Minus));
    }

    #[test]
    fn test_tokenize_case_insensitive_keywords() {
        let tokens = tokenize("set AND or").unwrap();
        assert!(matches!(tokens[0], Token::Set));
        assert!(matches!(tokens[1], Token::And));
        assert!(matches!(tokens[2], Token::Or));
    }
}
