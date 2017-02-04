extern crate walkdir;
#[macro_use]
extern crate log;
extern crate elp;

use std::path::Path;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use walkdir::{DirEntry, WalkDir, WalkDirIterator};
use elp::{ELBRecord, ParsingErrors};

pub type CounterResult<'a> = Result<ELBRecord<'a>, CounterError<'a>>;

/// Specific parsing errors that are returned as part of the [ParsingErrors::errors]
/// (struct.ParsingErrors.html) collection.
#[derive(Debug, PartialEq)]
pub enum CounterError<'a> {
    /// Returned if a line in an ELB file cannot be read.  Most likely the result of a bad file on
    /// disk.
    LineReadError,
    /// Returned if an ELB file cannot be opened.  Most likely the result of a bad file on disk.
    CouldNotOpenFile {
        path: String,
    },
    RecordParsingErrors(ParsingErrors<'a>),
}

impl<'a> Display for CounterError<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            CounterError::LineReadError => write!(f, "Unable to read a line."),
            CounterError::CouldNotOpenFile { ref path } => write!(f, "Unable to open file {}.", path),
            CounterError::RecordParsingErrors(ref errs) => write!(f, "Parsing errors: {:?}.", errs.errors),
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

/// A utility method for retrieving all of the paths to ELB log files in a directory.
///
/// If the user uses the [AWS S3 sync tool](http://docs.aws.amazon.com/cli/latest/reference/s3/sync.html)
/// to download their AWS ELB logs to a local disk the files will be in a very specific directory
/// hierarchy.  This utility will read the paths of the files, recursively searching a root
/// specified by the user, and append the paths to the `Vec<DirEntry>`, also provided by the user.
///
/// dir: The directory from which the paths of the ELB log files will be procured.
///
/// filenames: A Vec<DirEntry> to which the paths of the ELB log files will be written.
pub fn file_list(dir: &Path, filenames: &mut Vec<DirEntry>) -> Result<usize, walkdir::Error> {
    let dir_entries = WalkDir::new(dir)
        .min_depth(1)
        .into_iter()
        .filter_entry(|e| e.file_name().to_str().map(|s| s.ends_with(".log")).unwrap_or(false));
    for entry in dir_entries {
        let entry = entry?;
        filenames.push(entry);
    }
    Ok(filenames.len())
}

/// Attempt to parse every ELB record in every file in `filenames` and pass the results to the
/// record_handler.
///
/// Each file will be opened and each line, which should represent a ELB record, will be passed
/// through the parser.
///
/// # Failures
///
/// All failures including file access, file read, and parsing failures are passed to the
/// record_handler as a `ParsingErrors`.
pub fn process_files<H>(filenames: &[DirEntry], record_handler: &mut H) -> usize
    where H: FnMut(CounterResult) -> ()
{

    let mut total_record_count = 0;
    for filename in filenames {
        debug!("Processing file {}.", filename.path().display());
        match File::open(filename.path()) {
            Ok(file) => {
                let file_record_count = handle_file(file, record_handler);
                debug!("Found {} records in file {}.",
                file_record_count,
                filename.path().display());
                total_record_count += file_record_count;
            }

            Err(_) => {
                record_handler(Err(CounterError::CouldNotOpenFile {
                    path: format!("{}", filename.path().display()),
                }))
            }
        }
    }

    total_record_count
}

pub fn handle_file<H>(file: File, record_handler: &mut H) -> usize
    where H: FnMut(CounterResult) -> ()
{
    let mut file_record_count = 0;
    for possible_record in BufReader::new(&file).lines() {
        file_record_count += 1;
        match possible_record {
            Ok(record) => record_handler(
                elp::parse_record(&record).map_err(CounterError::RecordParsingErrors)
            ),

            Err(_) => {
                record_handler(Err(CounterError::LineReadError))
            }
        }
    }

    file_record_count
}