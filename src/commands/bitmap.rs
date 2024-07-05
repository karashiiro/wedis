use std::cmp;

use anyhow::Result;
use tracing::debug;

use crate::{
    connection::{ClientError, Connection},
    database::{DatabaseError, DatabaseOperations},
    indexing::adjust_indices,
};

fn mask_to_range(to_bit: usize) -> usize {
    (1 << to_bit) - 1
}

fn bit_range(data: &[u8], start_bit: usize, end_bit_exclusive: usize) -> Vec<u8> {
    if data.len() == 0 {
        return vec![];
    }

    let start_byte = start_bit / 8;
    let end_byte = cmp::min(data.len() - 1, end_bit_exclusive / 8);
    if end_byte < start_byte {
        return vec![];
    }

    /*
    Example (input: 5):
    0) 00010000 - initial state
    1) 00100000 - shift left by one
    2) 00011111 - subtract 1 (mask LSB)
    3) 11100000 - invert (optional, mask MSB instead of LSB)
    */
    let start_bitmask: u8 = (!mask_to_range(start_bit % 8) & 255).try_into().unwrap();

    let mut data_copy: Vec<u8> = vec![0; data.len()];
    data_copy[start_byte..=end_byte].copy_from_slice(&data[start_byte..=end_byte]);

    if end_byte != start_byte {
        data_copy[start_byte] &= start_bitmask;
    }

    let end_bitmask: u8 = mask_to_range(end_bit_exclusive % 8).try_into().unwrap();
    data_copy[end_byte] &= end_bitmask;

    data_copy
}

#[tracing::instrument(skip_all)]
pub fn bitcount(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 2 || args.len() > 5 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];

    match db.get_string(key) {
        Ok(value) => {
            let val = value.unwrap_or_default();
            debug!("Retrieved value {:?}", String::from_utf8_lossy(&val));

            if args.len() == 5 && String::from_utf8_lossy(&args[4]).to_uppercase() == "BIT" {
                let start = String::from_utf8_lossy(&args[2]).parse::<i64>()?;
                let end = String::from_utf8_lossy(&args[3]).parse::<i64>()?;

                let (start, end) = adjust_indices((val.len() * 8) - 1, start, end);
                let slice = bit_range(&val, start, end + 1);

                let bits: i64 = popcnt::count_ones(&slice).try_into().unwrap();
                Ok(conn.write_integer(bits))
            } else if args.len() >= 4 {
                let start = String::from_utf8_lossy(&args[2]).parse::<i64>()?;
                let end = String::from_utf8_lossy(&args[3]).parse::<i64>()?;
                let (start, end) = adjust_indices(val.len() - 1, start, end);
                let bits: i64 = popcnt::count_ones(&val[start..=end]).try_into().unwrap();
                Ok(conn.write_integer(bits))
            } else {
                let bits: i64 = popcnt::count_ones(&val).try_into().unwrap();
                Ok(conn.write_integer(bits))
            }
        }
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod test {
    use crate::{connection::MockConnection, database::MockDatabaseOperations};
    use mockall::predicate::*;

    use super::*;

    #[test]
    fn test_bitcount() {
        let key = "key";
        let value = "foobar";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(26))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["BITCOUNT".into(), key.into()];
        let _ = bitcount(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitcount_byte_unit() {
        let key = "key";
        let value = "foobar";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(6))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITCOUNT".into(),
            key.into(),
            1.to_string().into(),
            1.to_string().into(),
        ];
        let _ = bitcount(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitcount_bit_unit_1() {
        let key = "key";
        let value = "foobar";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(17))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITCOUNT".into(),
            key.into(),
            5.to_string().into(),
            30.to_string().into(),
            "BIT".into(),
        ];
        let _ = bitcount(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitcount_bit_unit_2() {
        let key = "key";
        let value = "foobar";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(|_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(1))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITCOUNT".into(),
            key.into(),
            1.to_string().into(),
            1.to_string().into(),
            "BIT".into(),
        ];
        let _ = bitcount(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_mask_to_range_zero() {
        assert_eq!(0, mask_to_range(0));
    }

    #[test]
    fn test_mask_to_range_1() {
        assert_eq!(0b00011111, mask_to_range(5));
    }

    #[test]
    fn test_mask_to_range_2() {
        assert_eq!(0b01111111, mask_to_range(31 % 8));
    }

    #[test]
    fn test_bit_range_1() {
        let data: Vec<u8> = vec![255; 3];
        let start_bit = 4;
        let end_bit = 19;

        let result = bit_range(&data, start_bit, end_bit + 1);
        assert_eq!(vec![0b11110000, 255, 0b00001111], result);
    }

    #[test]
    fn test_bit_range_2() {
        let data = "foobar";
        let start_bit = 5;
        let end_bit = 30;

        let result = bit_range(data.as_bytes(), start_bit, end_bit + 1);
        assert_eq!(
            vec![102 & 0b11110000, 111, 111, 98 & 0b01111111, 0, 0],
            result
        );
        assert_eq!(17, popcnt::count_ones(&result));
    }

    #[test]
    fn test_bit_range_3() {
        let data = "foobar";
        let start_bit = 1;
        let end_bit = 1;

        let result = bit_range(data.as_bytes(), start_bit, end_bit + 1);
        assert_eq!(vec![2, 0, 0, 0, 0, 0], result);
        assert_eq!(1, popcnt::count_ones(&result));
    }

    #[test]
    fn test_bit_range_empty() {
        let data: Vec<u8> = vec![];
        let start_bit = 4;
        let end_bit = 20;

        let result = bit_range(&data, start_bit, end_bit);
        let expected: Vec<u8> = vec![];
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bit_range_out_of_range() {
        let data: Vec<u8> = vec![255];
        let start_bit = 4;
        let end_bit = 20;

        let result = bit_range(&data, start_bit, end_bit);
        let expected: Vec<u8> = vec![0b00001111];
        assert_eq!(expected, result);
    }
}
