extern crate walkdir;

use std::path::Path;
use walkdir::{DirEntry, WalkDir, WalkDirIterator};

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