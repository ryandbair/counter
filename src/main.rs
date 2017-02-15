extern crate rustc_serialize;
extern crate elp;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate docopt;
extern crate chrono;
#[macro_use]
extern crate counter;

use docopt::Docopt;
use std::path;
use chrono::{DateTime, UTC};
use std::collections::HashMap;
use counter::{file_handling, record_handling};
use std::io::Write;

fn main() {
    env_logger::init().unwrap();
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    let log_location = &path::Path::new(&args.arg_log_location);
    debug!("Running summary on {}.", log_location.to_str().unwrap());

    let start: Option<DateTime<UTC>> = if args.flag_benchmark {
        Some(UTC::now())
    } else {
        None
    };

    let mut filenames = Vec::new();
    match file_handling::file_list(log_location, &mut filenames) {
        Ok(num_files) => {
            let mut agg: HashMap<record_handling::AggregateELBRecord, i64> = HashMap::new();
            debug!("Found {} files.", num_files);

            let number_of_records =
                file_handling::process_files(&filenames,
                                             &mut |counter_result: counter::CounterResult| {
                                                 record_handling::parsing_result_handler(
                                                     counter_result, &mut agg
                                                 );
                                             });
            debug!("Processed {} records in {} files.",
                   number_of_records,
                   num_files);

            for (aggregate, total) in &agg {
                println!("{},{},{},{}",
                         aggregate.system_name,
                         aggregate.day,
                         aggregate.client_address,
                         total);
            }

            if let Some(start_time) = start {
                let end_time = UTC::now();
                let time = end_time - start_time;
                println!("Processed {} files having {} records in {} milliseconds and produced \
                          {} aggregates.",
                         num_files,
                         number_of_records,
                         time.num_milliseconds(),
                         agg.len());
            }

            std::process::exit(0);
        }

        Err(e) => {
            println_stderr!("The following error occurred while trying to get the list of files. \
                             {}",
                            e);
            std::process::exit(1);
        }
    };
}

const USAGE: &'static str = "
counter

Usage:
  counter <log-location>
  counter (-b | --benchmark) <log-location>
  counter (-h | --help)

Options:
  -h --help         Show this screen.
  -b --benchmark    Turn on debug output
";

#[derive(Debug, RustcDecodable)]
struct Args {
    arg_log_location: String,
    flag_benchmark: bool,
}
