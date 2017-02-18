use std::collections::HashMap;
use urlparse::{Url, urlparse};
use std::io::Write;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregateELBRecord {
    pub day: String,
    pub client_address: String,
    pub system_name: String,
}

pub fn parsing_result_handler(counter_result: ::CounterResult,
                              aggregation: &mut HashMap<AggregateELBRecord, i64>)
                              -> () {
    match counter_result {
        Ok(elb_record) => {
            let url = urlparse(&elb_record.request_url);
            let aer = AggregateELBRecord {
                day: elb_record.timestamp.format("%Y-%m-%d").to_string(),
                client_address: elb_record.client_address.ip().to_string(),
                system_name: parse_system_name(&url)
                    .unwrap_or_else(|| "UNDEFINED_SYSTEM".to_owned()),
            };
            aggregate_record(aer, aggregation);
        }
        Err(::CounterError::RecordParsingErrors(ref errs)) => println_stderr!("{:?}", errs.record),
        Err(ref err) => println_stderr!("{:?}", err),
    }
}

fn parse_system_name(url: &Url) -> Option<String> {
    url.get_parsed_query()
        .map(|query_map| query_map.get("system").map(|systems| systems[0].clone()))
        .unwrap_or_else(|| None)
}

pub fn aggregate_records(new_aggs: &HashMap<AggregateELBRecord, i64>,
                     aggregation: &mut HashMap<AggregateELBRecord, i64>)
                     -> () {
    for (agg_key, agg_val) in new_aggs {
        let total = aggregation.entry(agg_key.clone()).or_insert(0);
        *total += *agg_val;
    }
 }

fn aggregate_record(aggregate_record: AggregateELBRecord,
                    aggregation: &mut HashMap<AggregateELBRecord, i64>)
                    -> () {
    let total = aggregation.entry(aggregate_record).or_insert(0);
    *total += 1;
}

#[cfg(test)]
mod record_handling_tests {

    use std::collections::HashMap;
    use super::AggregateELBRecord;
    use super::aggregate_record;

    const TEST_RECORD: &'static str = "2015-08-15T23:43:05.302180Z elb-name 172.16.1.6:54814 \
    172.16.1.5:9000 0.000039 0.145507 0.00003 200 200 0 7582 \
    \"GET http://some.domain.com:80/path0/path1?param0=p0&param1=p1 HTTP/1.1\"\
    ";

    #[test]
    fn inserting_two_records_with_different_values_creates_two_entries_each_recorded_once() {
        let mut agg: HashMap<AggregateELBRecord, i64> = HashMap::new();

        let ar0 = AggregateELBRecord {
            day: "2015-08-15".to_owned(),
            client_address: "172.16.1.6:54814".to_owned(),
            system_name: "sys1".to_owned(),
        };

        let ar1 = AggregateELBRecord {
            day: "2015-08-15".to_owned(),
            client_address: "172.16.1.6:54814".to_owned(),
            system_name: "sys2".to_owned(),
        };

        aggregate_record(ar0, &mut agg);
        aggregate_record(ar1, &mut agg);

        assert_eq!(agg.len(), 2);
        for (_, total) in agg {
            assert_eq!(total, 1)
        }
    }

    #[test]
    fn inserting_two_records_with_the_same_values_increases_the_total_correctly() {
        let mut agg: HashMap<AggregateELBRecord, i64> = HashMap::new();

        let ar0 = AggregateELBRecord {
            day: "2015-08-15".to_owned(),
            client_address: "172.16.1.6:54814".to_owned(),
            system_name: "sys1".to_owned(),
        };

        let ar1 = ar0.clone();
        let ar3 = ar0.clone();

        aggregate_record(ar0, &mut agg);
        aggregate_record(ar1, &mut agg);

        assert_eq!(agg[&ar3], 2);
    }
}
