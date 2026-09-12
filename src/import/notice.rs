//! What an import has to say, and how much it matters.
//!
//! Every message the importer raises carries a [`Concern`], decided at the
//! point it is raised. That decision is the only thing standing between "the
//! run printed a warning" and "the run refused to say it succeeded", and for a
//! while it was made afresh at each of thirty call sites. Now a message is
//! either something the run saw happen or it is not, and one function turns
//! that into an exit code.

use std::ops::Deref;

/// How much a message matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Concern {
    /// An original value was seen reaching the output. The run does not
    /// report success while any of these stand: `dynoxide import` exits 3,
    /// `--serve` and `--mcp` refuse to start, and `ImportSummary::exposures`
    /// lists them.
    ///
    /// Only for what the run observed, never for what it could not rule out.
    /// A caution raised on every ordinary import would make the non-zero
    /// exit the normal outcome, and the flag that suppresses it the default.
    Exposure,
    /// Something that could have gone wrong, or that the run could not
    /// check. Reported, and the exit code is unchanged.
    Caution,
}

/// One message from an import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub concern: Concern,
    pub message: String,
}

impl Notice {
    /// A value the run saw reach the output.
    pub fn exposure(message: impl Into<String>) -> Self {
        Self {
            concern: Concern::Exposure,
            message: message.into(),
        }
    }

    /// Something worth saying that does not change the exit code.
    pub fn caution(message: impl Into<String>) -> Self {
        Self {
            concern: Concern::Caution,
            message: message.into(),
        }
    }

    /// The same notice, named for the table it came from.
    pub fn for_table(mut self, table: &str) -> Self {
        self.message = format!("table '{table}': {}", self.message);
        self
    }
}

/// A notice reads as its message, so `notice.contains("...")` and the rest
/// of the string API work on it directly.
impl Deref for Notice {
    type Target = str;

    fn deref(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for Notice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
