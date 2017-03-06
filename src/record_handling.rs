use std::collections::HashMap;
use std::io::Write;
use std::marker::PhantomData;

use chrono::{Date, DateTime, UTC};
use std::hash::Hash;
use std::net::Ipv4Addr;
use std::mem;
use std::borrow::{Borrow, Cow};
use regex::Regex;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregateELBRecord<'a, TMType: MType> {
    pub day: Date<UTC>,
    pub client_address: Ipv4Addr,
    pub system_name: Option<Cow<'a, str>>,
    _phantom: PhantomData<TMType>,
}

impl<'a> AggregateELBRecord<'a, MTypeP> {
    fn new(day: DateTime<UTC>, client_address: Ipv4Addr, system: Option<&'a str>) -> AggregateELBRecord<'a, MTypeP> {
        AggregateELBRecord {
            day: day.date(),
            client_address: client_address,
            system_name: system.map(|v| Cow::Borrowed(v)), // TODO: use from
            _phantom: PhantomData,
        }
    }
}

impl<'a, 'b: 'a, T: MType> AggregateELBRecord<'a, T> {
    fn to_owned(&'a self) -> AggregateELBRecord<'b, MTypeH> {
        AggregateELBRecord {
            day: self.day,
            client_address: self.client_address,
            system_name: self.system_name.as_ref().map(|v| Cow::Owned((v.borrow() as &str).to_owned())),
            _phantom: PhantomData,
        }
    }
}

pub trait MType: Clone + Eq + Hash {}
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct MTypeH {}
impl MType for MTypeH {}
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct MTypeP {}
impl MType for MTypeP {}

pub fn parsing_result_handler<'a, 'b>(counter_result: ::CounterResult<'a>,
                              aggregation: &mut HashMap<AggregateELBRecord<'b, MTypeH>, i64>)
                              -> () {
    match counter_result {
        Ok(elb_record) => {
            let aer = AggregateELBRecord::new(
                elb_record.timestamp,
                *elb_record.client_address.ip(),
                parse_system_name_regex(&elb_record.request_url)
            );
            aggregate_record(&aer, aggregation);
        }
        Err(::CounterError::RecordParsingErrors(ref errs)) => println_stderr!("{:?}", errs.record),
        Err(ref err) => println_stderr!("{:?}", err),
    }
}

lazy_static! {
    static ref SYSTEM_REGEX: Regex = Regex::new(r"(?i)system=([^&]*)").unwrap();
}

impl<'a, 'b> Borrow<AggregateELBRecord<'b, MTypeP>> for AggregateELBRecord<'a, MTypeH> {
    fn borrow(&self) -> &AggregateELBRecord<'b, MTypeP> {
        unsafe {
            mem::transmute(self)
        }
    }
}

fn parse_system_name_regex(q: &str) -> Option<&str> {
    SYSTEM_REGEX.captures(q).and_then( |cap| cap.get(1).map(|sys| sys.as_str() ))
}

pub fn aggregate_records<'a>(new_aggs: &HashMap<AggregateELBRecord<'a, MTypeH>, i64>,
                     aggregation: &mut HashMap<AggregateELBRecord<'a, MTypeH>, i64>)
                     -> () {
    for (agg_key, agg_val) in new_aggs {
        let present = if let Some(c) = aggregation.get_mut(agg_key) {
            *c += *agg_val;
            true
        } else {
            false
        };
        if !present {
            aggregation.insert(agg_key.to_owned(), *agg_val);
        }
    }
 }

fn aggregate_record<'a, 'b: 'a>(aggregate_record: &AggregateELBRecord<'a, MTypeP>,
                    aggregation: &mut HashMap<AggregateELBRecord<'b, MTypeH>, i64>)
                    -> () {
    let present = if let Some(c) = aggregation.get_mut(aggregate_record) {
        *c += 1;
        true
    } else {
        false
    };
    if !present {
        aggregation.insert(aggregate_record.to_owned(), 1);
    }
}

#[cfg(test)]
mod record_handling_tests {

    use std::collections::HashMap;
    use std::net::Ipv4Addr;

    use chrono::{UTC, Date, TimeZone};

    use super::*;

    const TEST_RECORD: &'static str = "2015-08-15T23:43:05.302180Z elb-name 172.16.1.6:54814 \
    172.16.1.5:9000 0.000039 0.145507 0.00003 200 200 0 7582 \
    \"GET http://some.domain.com:80/path0/path1?param0=p0&param1=p1 HTTP/1.1\"\
    ";

    #[test]
    fn inserting_two_records_with_different_values_creates_two_entries_each_recorded_once() {
        let mut agg: HashMap<AggregateELBRecord<MTypeH>, i64> = HashMap::new();

        let ar0 = AggregateELBRecord {
            day: UTC.ymd(2015, 8, 15),
            client_address: Ipv4Addr::new(172, 16, 1, 6),
            system_name: Some("sys1".into()),
            _phantom: PhantomData::<MTypeP>,
        };

        let ar1 = AggregateELBRecord {
            day: UTC.ymd(2015, 8, 15),
            client_address: Ipv4Addr::new(172, 16, 1, 6),
            system_name: Some("sys2".into()),
            _phantom: PhantomData::<MTypeP>,
        };

        aggregate_record(&ar0, &mut agg);
        aggregate_record(&ar1, &mut agg);

        assert_eq!(agg.len(), 2);
        for (_, total) in agg {
            assert_eq!(total, 1)
        }
    }

    #[test]
    fn inserting_two_records_with_the_same_values_increases_the_total_correctly() {
        let mut agg: HashMap<AggregateELBRecord<MTypeH>, i64> = HashMap::new();

        let ar0 = AggregateELBRecord {
            day: UTC.ymd(2015, 8, 15),
            client_address: Ipv4Addr::new(172, 16, 1, 6),
            system_name: Some("sys1".into()),
            _phantom: PhantomData::<MTypeP>,
        };

        let ar1 = ar0.clone();
        let ar3 = ar0.clone();

        aggregate_record(&ar0, &mut agg);
        aggregate_record(&ar1, &mut agg);

        assert_eq!(agg[&ar3], 2);
    }
}
