mod timebase;
mod timeseries;

use crate::timebase::{GetDataResponse, TagValue, TimebaseClient};
use crate::timeseries::{DataPoint, DataQuality, DataSeries, DataValue};
use chrono::{DateTime, Days, Local, Months, Utc};
use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct EventInfo {
    pub name: String
}

#[derive(Debug)]
pub struct Event {
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub attributes: HashMap<String, String>
}

#[derive(Debug)]
pub struct EventSeries {
    pub info: EventInfo,
    pub events: Vec<Event>
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parameters for the request
    let base_url = "http://localhost:4511";
    let dataset_name = "The Juice Factory";
    let tag_names = vec!["131-FQ-001.PV", "131-FT-001.PV", "FL001.State", "FL001.Product", "FL001.BatchId"];
    let start_time_str = "2025-11-01T00:00:00.00000-05:00";

    // Create a DateTime<Utc> from an ISO/RFC3339 string
    let start_time = DateTime::parse_from_rfc3339(start_time_str)?
        .with_timezone(&Utc);

    let end_time = start_time.checked_add_months(Months::new(1)).unwrap().to_utc();
    // let end_time = start_time.checked_add_days(Days::new(7)).unwrap().to_utc();
    // let end_time = Local::now();

    // Create a TimebaseClient
    let client = TimebaseClient::from_str(base_url)?
        .set_timeout(Duration::from_secs(30));

    // Send the request
    let response = client.get_data(dataset_name)
        .tag_names(&tag_names)
        .start(start_time)
        .end(end_time)
        .build()?
        .send().await?;

    println!("Response received. Processing data...");

    // Process the response
    let time_series = response.time_series();


    let mut dp = Vec::new();
    time_series.iter().for_each(|tag| {
        dp.extend(tag.data
            .iter()
            .map(|dp| (&tag.tag, dp)));
    });

    dp.sort_by_key(|&(_, d)| { d.timestamp });

    let mut timestamp = start_time;
    let mut last_values = time_series
        .iter()
        .map(|t| (t.tag.name.clone(), None::<DataValue>))
        .collect::<HashMap<_, _>>();

    let mut data_table: Vec<(DateTime<Utc>, Vec<Option<DataValue>>)> = Vec::new();

    dp.iter().for_each(|(tag, dp)| {

        if dp.timestamp > timestamp {
            let values: Vec<Option<DataValue>> = last_values.values().cloned().collect();   
            
            data_table.push((timestamp, values));
        }
        
        timestamp = dp.timestamp;

        last_values.insert(tag.name.clone(), dp.value.clone());
    });
    
    data_table.iter().take(10).for_each(|(ts, values)| println!("{}: {:?}", ts.to_rfc3339(), values));

    dp.iter().take(10).for_each(|(tag, dp)| println!("{} {}: {:?}, {:?}", dp.timestamp.to_rfc3339(), tag.name, dp.value, dp.quality));


    println!("Data Points: {}", dp.len());


    let start = Instant::now();
    let mut test_timestamp = start_time;
    let mut hour_counter = 0;
    while test_timestamp < response.end {
        let test_value = time_series[4].get_value_at(test_timestamp);
        println!("Value of \"{}\" at {}: {:?}", time_series[4].tag.name, test_timestamp.to_rfc3339(), test_value);
        test_timestamp = test_timestamp.add(chrono::Duration::hours(1));
        hour_counter += 1;
    }
    let duration = start.elapsed();
    println!("Time elapsed in while loop: {:?}", duration);
    println!("Number of hours in while loop: {}", hour_counter);

    Ok(())
}

impl GetDataResponse {
    fn time_series(&self) -> Vec<DataSeries> {
        self.tags.iter().map(|tl| {
            // 4. Return the data points in our own data model
            DataSeries {
                tag: crate::timeseries::Tag {
                    name: tl.tag.name.clone(),
                    description: tl.tag.description.clone(),
                    format: tl.tag.format.clone(),
                    uom: match &tl.tag.uom {
                        None => Default::default(),
                        Some(uom) => match uom.len() {
                            1 => Some(uom.values().next().unwrap().clone()),
                            _ => Default::default()
                        }
                    },
                    states: match &tl.tag.uom {
                        None => Default::default(),
                        Some(uom) => match uom.len() {
                            n if n > 1 => {
                                uom.iter().map(|(k, v)| (*k, v.clone())).collect()
                            },
                            _ => Default::default()
                        }
                    },
                    fields: tl.tag.fields.clone().unwrap_or_default(),
                },
                data: tl.data.iter().map(|dp| {
                    DataPoint {
                        timestamp: dp.timestamp,
                        value: match &dp.value {
                            Some(TagValue::Integer(v)) => Some(DataValue::Integer(*v)),
                            Some(TagValue::Float(v)) => Some(DataValue::Float(*v)),
                            Some(TagValue::Text(v)) => Some(DataValue::Text(v.clone())),
                            None => None,
                        },
                        quality: match dp.quality {
                            n if n & 0xC0 >= 0 => DataQuality::Good(n),
                            _ => DataQuality::Bad(dp.quality)
                        },
                    }
                }).collect()
            }
        }).collect()
    }
}