use super::{RedisType, RedisValue};

#[derive(Clone)]
pub struct RedisString(pub Vec<u8>);

impl TryFrom<Vec<u8>> for RedisString {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(value))
    }
}

impl From<RedisString> for Vec<u8> {
    fn from(value: RedisString) -> Self {
        value.0
    }
}

impl From<RedisString> for RedisValue<RedisString> {
    fn from(value: RedisString) -> Self {
        RedisValue::new(RedisType::String, value)
    }
}

impl RedisString {
    pub fn new(value: &[u8]) -> Self {
        let mut data: Vec<u8> = vec![];
        value.clone_into(&mut data);
        Self(data)
    }
}
