use std::cmp::Ordering;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use crate::timebase::TagItem;

#[derive(Debug)]
#[derive(Clone)]
pub enum DataValue {
    Integer(i32),
    Float(f64),
    Text(String)
}

#[derive(Debug)]
pub struct Tag {
    pub name: String,
    pub description: Option<String>,
    pub format: Option<String>,
    pub uom: Option<String>,
    pub states: HashMap<i32, String>,
    pub fields: HashMap<String, String>,
}

#[derive(Debug)]
pub enum DataQuality {
    Good(i16),
    Bad(i16),
    Unknown(i16)
}

#[derive(Debug)]
pub struct DataPoint {
    pub timestamp: DateTime<Utc>,
    pub value: Option<DataValue>,
    pub quality: DataQuality
}

#[derive(Debug)]
pub struct DataSeries {
    pub tag: Tag,
    pub data: Vec<DataPoint>
}

#[derive(Debug)]
pub struct DataPoint2<T> {
    pub timestamp: DateTime<Utc>,
    pub value: Option<T>,
}

#[derive(Debug)]
struct TimeSlice<'a, T> {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    data: &'a [DataPoint2<T>]
}


impl DataSeries {
    pub fn get_value_at(&self, timestamp: DateTime<Utc>) -> Option<&DataValue> {
        if self.data.is_empty() {
            return None;
        }

        let mut min = 0;
        let mut max = self.data.len() - 1;
        let mut index = (min + max) / 2;

        while max - min > 1 {
            match self.data[index].timestamp.cmp(&timestamp) {
                Ordering::Less => min = index,
                Ordering::Greater => max = index,
                Ordering::Equal => {
                    min = index;
                    max = index;
                }
            }
            index = (min + max) / 2;
        }

        self.data[min].value.as_ref()
    }

    fn slice(&self, sections: Vec<DateTime<Utc>>) -> Vec<DataPointSlice<'_>> {
        vec![&self.data[0..1]]
    }
}

trait Aggregatable {
    fn aggregate<T>(&self) -> T;
}

type DataPointSlice<'a> = &'a [DataPoint];

impl Aggregatable for DataPointSlice<'_> {
    fn aggregate<T>(&self) -> T {
        todo!()
    }
}

impl From<&TagItem> for Vec<DataPoint2<i32>> {
    fn from(item: &TagItem) -> Self {
        item.data.iter().map(|d| {
            DataPoint2 {
                timestamp: d.timestamp,
                value: match &d.value {
                    None => None,
                    Some(crate::timebase::TagValue::Integer(v)) => Some(*v),
                    Some(crate::timebase::TagValue::Float(v)) => Some(v.round() as i32),
                    Some(crate::timebase::TagValue::Text(v)) => match v.parse::<i32>() {
                        Ok(v) => Some(v),
                        Err(_) => None
                    },
                },
            }
        }).collect()
    }
}

impl TagItem {
    pub fn get_data_points<T>(&self) -> Vec<DataPoint2<T>>
    where
        T: From<i32> + From<f64> + From<String> + Copy + std::fmt::Debug
    {
        self.data.iter().map(|d| {
            DataPoint2 {
                timestamp: d.timestamp,
                value: match &d.value {
                    None => None,
                    Some(crate::timebase::TagValue::Integer(v)) => Some(T::from(*v)),
                    _ => Some(T::from(0))
                },
            }
        }).collect()
    }
}