extern crate walkdir;
#[macro_use]
extern crate log;
extern crate elp;
extern crate urlparse;
extern crate chrono;

use std::fmt;
use std::fmt::{Display, Formatter};
use std::error::Error;

#[macro_export]
macro_rules! println_stderr(
    ($($arg:tt)*) => (
        match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
            Ok(_) => {},
            Err(x) => panic!("Unable to write to stderr: {}", x),
        }
    )
);

pub mod file_handling;
pub mod record_handling;

pub type CounterResult<'a> = Result<elp::ELBRecord<'a>, CounterError<'a>>;

/// Specific parsing errors that are returned as part of the [`ParsingErrors::errors`]
/// (struct.ParsingErrors.html) collection.
#[derive(Debug, PartialEq)]
pub enum CounterError<'a> {
    /// Returned if a line in an ELB file cannot be read.  Most likely the result of a bad file on
    /// disk.
    LineReadError,
    /// Returned if an ELB file cannot be opened.  Most likely the result of a bad file on disk.
    CouldNotOpenFile { path: String },
    RecordParsingErrors(elp::ParsingErrors<'a>),
}

impl<'a> Display for CounterError<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            CounterError::LineReadError => write!(f, "Unable to read a line."),
            CounterError::CouldNotOpenFile { ref path } => {
                write!(f, "Unable to open file {}.", path)
            }
            CounterError::RecordParsingErrors(ref errs) => {
                write!(f, "Parsing errors: {:?}.", errs.errors)
            }
        }
    }
}

impl<'a> Error for CounterError<'a> {
    fn description(&self) -> &str {
        match *self {
            CounterError::LineReadError => "failed to read line",
            CounterError::CouldNotOpenFile { .. } => "failed to open file",
            CounterError::RecordParsingErrors(_) => "failed to parse record",
        }
    }
}
