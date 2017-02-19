extern crate rustc_serialize;
extern crate elp;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate clap;
extern crate chrono;
#[macro_use]
extern crate counter;
extern crate num_cpus;
extern crate scoped_pool as sp;
extern crate walkdir;

use std::path;
use chrono::{DateTime, UTC};
use std::collections::HashMap;
use counter::{file_handling, record_handling};
use std::io::Write;
use std::sync::mpsc;
use walkdir::DirEntry;

// TODO: Refactor and test this code.
// TODO: Try the pub/sub version after this version is working and tested properly.
fn main() {
    env_logger::init().unwrap();

    // TODO: Move this to a fn.
    let args = clap::App::new("counter")
        .arg(clap::Arg::with_name(LOG_LOCATION_ARG)
            .required(true)
            .help("The root directory when the log files are stored."))
        .arg(clap::Arg::with_name(BENCHMARK_ARG)
            .required(false)
            .help("Time the run and provide statistics at the end of the run.")
            .long("benchmark")
            .short("b"))
        .get_matches();

    let log_location = &path::Path::new(args.value_of(LOG_LOCATION_ARG).unwrap());
    debug!("Running summary on {}.", log_location.to_str().unwrap());

    let start: Option<DateTime<UTC>> = if args.is_present(BENCHMARK_ARG) {
        Some(UTC::now())
    } else {
        None
    };

    let pool = sp::Pool::new(num_cpus::get());

    let mut filenames = Vec::new();
    let exit_code = match file_handling::file_list(log_location, &mut filenames) {
        Ok(num_files) => {
            let mut agg: HashMap<record_handling::AggregateELBRecord, i64> = HashMap::new();
            debug!("Found {} files.", num_files);
            let mut filename_senders = Vec::new();
            let (agg_sender, agg_receiver) = mpsc::channel::<_>();
            for sender_id in 0..pool.workers() {
                let (filename_sender, filename_receiver) = mpsc::channel::<_>();
                filename_senders.push(filename_sender);
                let cloned_agg_sender = agg_sender.clone();
                pool.spawn(move ||  run_file_processor(sender_id, filename_receiver, cloned_agg_sender) );
            }

            let mut number_of_records = 0;
            let mut dones = 0;
            while dones < pool.workers() {
                match agg_receiver.recv() {
                    Ok(AggregationMessages::Start(sender_id)) => {
                        let sender = &filename_senders[sender_id];
                        if let Some(filename) = filenames.pop() {
                            let _ = sender.send(ParsingMessages::Filename(filename));
                        } else {
                            let _ = sender.send(ParsingMessages::Done);
                        }
                    },
                    Ok(AggregationMessages::Aggregate(num_parsed_records, new_agg, sender_id)) => {
                        debug!("Received new_agg having {} records.", new_agg.len());
                        let sender = &filename_senders[sender_id];
                        if let Some(filename) = filenames.pop() {
                            let _ = sender.send(ParsingMessages::Filename(filename));
                        } else {
                            let _ = sender.send(ParsingMessages::Done);
                        }
                        number_of_records += num_parsed_records;
                        record_handling::aggregate_records(&new_agg, &mut agg);
                    },
                    Ok(AggregationMessages::Done) => dones += 1,
                    Err(_) => debug!("Received an error from one of the parsing workers."),
                }
            }

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
            EXIT_SUCCESS
        }

        Err(e) => {
            println_stderr!("The following error occurred while trying to get the list of files. \
                             {}",
                            e);
            EXIT_FAILURE
        }
    };

    pool.shutdown();
    std::process::exit(exit_code);
}

enum AggregationMessages {
    Start(usize),
    Aggregate(usize, HashMap<record_handling::AggregateELBRecord, i64>, usize),
    Done
}

enum ParsingMessages {
    Filename(DirEntry),
    Done
}

// TODO: Test this.
// TODO: Use a real file.
fn run_file_processor(id: usize,
                      filename_receiver: mpsc::Receiver<ParsingMessages>,
                      aggregate_sender: mpsc::Sender<AggregationMessages>) -> () {
    let mut done = false;
    // TODO: There needs to be a timeout here to ensure the program doesn't run forever.
    // TODO: Make use of try_rec.
    // TODO: Report a timeout back to main.
    let _ = aggregate_sender.send(AggregationMessages::Start(id));
    while !done {
        done = match filename_receiver.recv() {
            Ok(ParsingMessages::Filename(filename)) => {
                debug!("Received filename {}.", filename.path().display());
                let mut agg: HashMap<record_handling::AggregateELBRecord, i64> = HashMap::new();
                let num_records = file_handling::process_file(&filename,
                  &mut |counter_result: counter::CounterResult| {
                      record_handling::parsing_result_handler(
                          counter_result, &mut agg
                      );
                  });
                debug!("Found {} aggregates in {}.", agg.len(), filename.path().display());
                let _ = aggregate_sender.send(AggregationMessages::Aggregate(num_records, agg, id));
                false
            },
            Ok(ParsingMessages::Done) => true,
            Err(_) => true,
        }
    }
    let _ = aggregate_sender.send(AggregationMessages::Done);
}

const LOG_LOCATION_ARG: &'static str = "log-location";
const BENCHMARK_ARG: &'static str = "benchmark";
const EXIT_SUCCESS: i32 = 0;
const EXIT_FAILURE: i32 = 1;
