use anyhow::anyhow;

#[derive(PartialEq, Copy, Clone)]
pub enum RedisType {
    String = 1,
}

impl From<RedisType> for u8 {
    fn from(value: RedisType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for RedisType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            x if x == RedisType::String as u8 => Ok(RedisType::String),
            _ => Err(anyhow!("Failed to convert u8 to RedisType")),
        }
    }
}

pub trait TrySerialize = Clone + TryFrom<Vec<u8>, Error = anyhow::Error>;

// TODO: Make value a reference?
pub struct RedisValue<T: TrySerialize> {
    type_id: RedisType,
    value: T,
}

impl<T: TrySerialize> RedisValue<T>
where
    Vec<u8>: From<T>,
{
    pub fn new(type_id: RedisType, value: T) -> Self {
        Self { type_id, value }
    }

    pub fn is_type(&self, other: RedisType) -> bool {
        self.type_id == other
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn as_vec(&self) -> Vec<u8> {
        let mut data: Vec<u8> = vec![self.type_id.into()];
        let mut value_data: Vec<u8> = self.value.clone().into();
        data.append(&mut value_data);
        data
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, anyhow::Error> {
        let type_id = RedisType::try_from(bytes[0])?;
        let mut data: Vec<u8> = vec![];
        bytes[1..].clone_into(&mut data);

        Ok(Self {
            type_id,
            value: data.try_into()?,
        })
    }
}
