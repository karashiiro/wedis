use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::trace;

#[derive(Error, Debug)]
pub enum TimeError {
    #[error("integer parse error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("integer conversion error")]
    ConvertInt(#[from] std::num::TryFromIntError),
    #[error("system time is before UNIX epoch")]
    SystemTimeBeforeUnix(#[from] std::time::SystemTimeError),
}

pub fn unix_timestamp() -> Result<Duration, TimeError> {
    Ok(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?)
}

pub fn serialize_duration_as_timestamp(duration: Duration) -> Result<Vec<u8>, TimeError> {
    let total_ms: i64 = (unix_timestamp()? + duration).as_millis().try_into()?;
    let total_ms = total_ms.to_string();
    trace!("Serialized duration: {}", total_ms);
    Ok(total_ms.to_string().into_bytes())
}

pub fn parse_timestamp(timestamp: &[u8]) -> Result<Duration, TimeError> {
    let timestamp = String::from_utf8_lossy(&timestamp)
        .parse::<i64>()
        .and_then(|ts| {
            Ok(Duration::from_millis(
                ts.try_into().expect("Failed to convert timestamp to i64"),
            ))
        })?;
    trace!("Parsed duration: {:?}", timestamp);
    Ok(timestamp)
}
