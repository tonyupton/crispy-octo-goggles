use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
// This module contains all structs and enums related to the timebase data model

#[derive(Serialize, Deserialize, Debug)]
pub struct Tag {
    #[serde(rename = "n")]
    pub name: String,

    #[serde(rename = "d")]
    pub description: Option<String>,

    #[serde(rename = "f")]
    pub format: Option<String>,

    #[serde(rename = "u")]
    pub uom: Option<HashMap<i32, String>>,

    #[serde(rename = "fl")]
    pub fields: Option<HashMap<String, String>>,

    #[serde(rename = "t")]
    pub data_type: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TagData {
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,

    #[serde(rename = "v")]
    pub value: Option<TagValue>,

    #[serde(rename = "q")]
    pub quality: i16,
}

// The get_value can be either a number or a string in incoming JSON. Use an untagged enum
// so Serde will accept either representation seamlessly.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum TagValue {
    Integer(i32),
    Float(f64),
    Text(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TagItem {
    #[serde(rename = "t")]
    pub tag: Tag,

    #[serde(rename = "d")]
    pub data: Vec<TagData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetDataResponse {
    #[serde(rename = "s")]
    pub start: DateTime<Utc>,

    #[serde(rename = "e")]
    pub end: DateTime<Utc>,

    #[serde(rename = "tl")]
    pub tags: Vec<TagItem>,
}


pub struct TimebaseClient {
    base_url: Url,
    timeout: Duration,
}

impl TimebaseClient {
    pub fn new() -> Self {
        Self {
            base_url: match Url::parse("http://localhost:4511") {
                Ok(url) => url,
                Err(_) => panic!("Invalid base URL")
            },
            timeout: Duration::from_secs(30)
        }
    }

    pub fn from_url(base_url: &Url) -> Self {
        Self {
            base_url: base_url.clone(),
            timeout: Duration::from_secs(30)
        }
    }

    pub fn from_str(base_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            base_url: match Url::parse(base_url) {
                Ok(url) => url,
                Err(_) => return Err("Invalid base URL".into())
            },
            timeout: Duration::from_secs(30)
        })
    }

    pub fn from_host(host: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            base_url: Url::parse(format!("http://{}:4511", host).as_str())?,
            timeout: Duration::from_secs(30)
        })
    }

    pub fn set_host(mut self, host: &str) -> Result<Self, Box<dyn std::error::Error>> {
        match self.base_url.set_host(Some(host)) {
            Ok(_) => Ok(self),
            Err(_) => Err("Invalid host".into())
        }
    }

    pub fn set_scheme(mut self, scheme: &str) -> Result<Self, Box<dyn std::error::Error>> {
        match self.base_url.set_scheme(scheme) {
            Ok(_) => Ok(self),
            Err(_) => Err("Invalid scheme".into())
        }
    }

    pub fn set_port(mut self, port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        match self.base_url.set_port(Some(port)) {
            Ok(_) => Ok(self),
            Err(_) => Err("Invalid port".into())
        }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn get_data<'a>(&'a self, dataset: &'a str) -> GetDataRequestBuilder<'a> {
        GetDataRequestBuilder {
            client: self,
            dataset_name: dataset,
            start: None,
            end: None,
            relative_start: None,
            relative_end: None,
            tag_names: vec![],
        }
    }
}


pub struct GetDataRequestBuilder<'a> {
    client: &'a TimebaseClient,
    dataset_name: &'a str,
    start: Option<DateTime<FixedOffset>>,
    end: Option<DateTime<FixedOffset>>,
    relative_start: Option<&'a str>,
    relative_end: Option<&'a str>,
    tag_names: Vec<&'a str>,
}

impl<'a> GetDataRequestBuilder<'a> {
    pub fn start<T: TimeZone>(mut self, time: DateTime<T>) -> Self {
        self.start = Some(time.fixed_offset());
        self
    }

    pub fn end<T: TimeZone>(mut self, time: DateTime<T>) -> Self {
        self.end = Some(time.fixed_offset());
        self
    }

    pub fn start_iso(mut self, start: &'a str) -> Result<Self, Box<dyn std::error::Error>> {
        self.start = Some(DateTime::parse_from_rfc3339(start)?);
        Ok(self)
    }

    pub fn tag_name(mut self, tag_name: &'a str) -> Self {
        self.tag_names.push(tag_name);
        self
    }

    pub fn tag_names(mut self, tag_names: &'a Vec<&'a str>) -> Self {
        self.tag_names.extend(tag_names);
        self
    }

    pub fn build(self) -> Result<GetDataRequest, Box<dyn std::error::Error>> {
        let mut url = self.client.base_url.clone().join(&format!("api/datasets/{}/data", self.dataset_name))?;

        {
            let mut query_pairs = url.query_pairs_mut();

            self.tag_names.iter().for_each(|tag_name| {
                query_pairs.append_pair("tagname", tag_name);
            });

            if let Some(start) = self.start {
                query_pairs.append_pair("start", start.to_rfc3339().as_str());
            }

            if let Some(end) = self.end {
                query_pairs.append_pair("end", end.to_rfc3339().as_str());
            }
        }

        Ok(GetDataRequest { url, timeout: self.client.timeout })
    }
}

pub struct GetDataRequest {
    url: Url,
    timeout: Duration,
}

impl GetDataRequest {
    pub async fn send(&self) -> Result<GetDataResponse, Box<dyn std::error::Error>> {
        let url = self.url.clone();
        let client = Client::builder()
            .timeout(self.timeout)
            .build()?;

        println!("GET {}", url);

        let resp = client.get(url).send().await?;

        if !resp.status().is_success() {
            return Err(format!("HTTP request failed with status code {}", resp.status()).into());
        }

        let data: GetDataResponse = resp.json().await?;

        Ok(data)
    }
}