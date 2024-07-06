use std::cmp;

use anyhow::Result;
use tracing::debug;

use crate::{
    connection::{ClientError, Connection},
    database::{DatabaseError, DatabaseOperations},
    indexing::adjust_indices,
};

fn bit_to_mask(to_bit: usize) -> usize {
    (1 << to_bit) - 1
}

fn bit_range(data: &[u8], start_bit: usize, mut end_bit_exclusive: usize) -> Vec<u8> {
    if data.len() == 0 {
        return vec![];
    }

    let start_byte = start_bit / 8;
    let mut end_byte = end_bit_exclusive / 8;
    if end_byte > data.len() - 1 {
        end_byte = data.len() - 1;
        end_bit_exclusive = data.len() * 8;
    }

    if end_byte < start_byte {
        return vec![];
    }

    /*
    Example (input: 5):
    0) 00010000 - initial state
    1) 00100000 - shift left by one
    2) 00011111 - subtract 1 (mask LSB)
    3) 11111000 - reverse (mask MSB instead of LSB)
    */
    let start_bitmask: u8 = bit_to_mask(start_bit % 8).try_into().unwrap();
    let start_bitmask = start_bitmask.reverse_bits();
    let end_bitmask: u8 = if end_bit_exclusive % 8 == 0 && end_bit_exclusive != 0 {
        255
    } else {
        bit_to_mask(end_bit_exclusive % 8).try_into().unwrap()
    };
    let end_bitmask = end_bitmask.reverse_bits();

    let mut data_copy: Vec<u8> = vec![0; data.len()];
    data_copy[start_byte..=end_byte].copy_from_slice(&data[start_byte..=end_byte]);

    if end_byte == start_byte {
        let mask = !start_bitmask & end_bitmask;
        data_copy[start_byte] &= mask;
    } else {
        data_copy[start_byte] &= !start_bitmask;
        data_copy[end_byte] &= end_bitmask;
    }

    data_copy
}

fn find_first_bit_pos_byte(bale: u8, needle: u8) -> Option<usize> {
    // Iterate from MSB to LSB
    for i in (0..8).rev() {
        let bit = (bale & (1 << i)) >> i;
        if needle == bit {
            return Some(7 - i);
        }
    }

    None
}

fn find_first_bit_pos(
    haystack: &[u8],
    needle: u8,
    start_bit: usize,
    end_bit_exclusive: usize,
) -> Option<usize> {
    if end_bit_exclusive <= start_bit {
        return None;
    }

    for (i, bale) in haystack.iter().enumerate() {
        let pos = find_first_bit_pos_byte(*bale, needle).and_then(|p| Some(i * 8 + p));
        if let Some(pos) = pos {
            if pos >= start_bit && pos < end_bit_exclusive {
                return Some(pos);
            }
        }
    }

    None
}

#[tracing::instrument(skip_all)]
pub fn bitpos(
    conn: &mut dyn Connection,
    db: &dyn DatabaseOperations,
    args: &Vec<Vec<u8>>,
) -> Result<()> {
    if args.len() < 3 || args.len() > 6 {
        conn.write_error(ClientError::ArgCount);
        return Ok(());
    }

    let key = &args[1];
    let bit: u8 = if &args[2] == "1".as_bytes() { 1 } else { 0 };
    match db.get_string(key) {
        Ok(value) => {
            let val = value.unwrap_or_default();
            debug!("Retrieved value {:?}", String::from_utf8_lossy(&val));

            if args.len() == 6 && String::from_utf8_lossy(&args[5]).to_uppercase() == "BIT" {
                let start = String::from_utf8_lossy(&args[3]).parse::<i64>()?;
                let end = String::from_utf8_lossy(&args[4]).parse::<i64>()?;

                let (new_start, new_end) = adjust_indices(val.len() * 8 - 1, start, end);

                let pos = find_first_bit_pos(&val, bit, new_start, new_end + 1)
                    .and_then(|p| Some(p as i64))
                    .unwrap_or(-1);
                Ok(conn.write_integer(pos))
            } else if args.len() >= 5 {
                let start = String::from_utf8_lossy(&args[3]).parse::<i64>()?;
                let end = String::from_utf8_lossy(&args[4]).parse::<i64>()?;

                let (new_start, new_end) = adjust_indices(val.len() - 1, start, end);

                let pos = find_first_bit_pos(&val, bit, new_start * 8, new_end * 8)
                    .and_then(|p| Some(p as i64))
                    .unwrap_or(-1);
                Ok(conn.write_integer(pos))
            } else if args.len() >= 4 {
                let start = String::from_utf8_lossy(&args[3]).parse::<i64>()?;
                let end: i64 = cmp::max(val.len() as i64 - 1, 0);

                let (new_start, new_end) = adjust_indices(val.len() - 1, start, end);

                let pos = find_first_bit_pos(&val, bit, new_start * 8, new_end * 8)
                    .and_then(|p| Some(p as i64))
                    .unwrap_or(val.len() as i64);
                Ok(conn.write_integer(pos))
            } else {
                let pos =
                    find_first_bit_pos(&val, bit, 0, val.len() * 8).unwrap_or(val.len()) as i64;
                Ok(conn.write_integer(pos))
            }
        }
        Err(DatabaseError::WrongType { expected: _ }) => {
            Ok(conn.write_error(ClientError::WrongType))
        }
        Err(err) => Err(err.into()),
    }
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
    fn test_bit_to_mask_zero() {
        assert_eq!(0, bit_to_mask(0));
    }

    #[test]
    fn test_bit_to_mask_1() {
        assert_eq!(0b00011111, bit_to_mask(5));
    }

    #[test]
    fn test_bit_to_mask_2() {
        assert_eq!(0b01111111, bit_to_mask(31 % 8));
    }

    #[test]
    fn test_bit_range_1() {
        let data: Vec<u8> = vec![255; 3];
        let start_bit = 4;
        let end_bit = 19;

        let result = bit_range(&data, start_bit, end_bit + 1);
        assert_eq!(vec![0b00001111, 255, 0b11110000], result);
    }

    #[test]
    fn test_bit_range_2() {
        let data = "foobar";
        let start_bit = 5;
        let end_bit = 30;

        let result = bit_range(data.as_bytes(), start_bit, end_bit + 1);
        assert_eq!(
            vec![102 & 0b00000111, 111, 111, 98 & 0b11111110, 0, 0],
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
        assert_eq!(vec![0b01000000, 0, 0, 0, 0, 0], result);
        assert_eq!(1, popcnt::count_ones(&result));
    }

    #[test]
    fn test_bit_range_4() {
        let data: Vec<u8> = vec![0xFF];
        let start_bit = 1;
        let end_bit = 3;

        let result = bit_range(&data, start_bit, end_bit + 1);
        assert_eq!(vec![0b01110000], result);
        assert_eq!(3, popcnt::count_ones(&result));
    }

    #[test]
    fn test_bit_range_empty() {
        let data: Vec<u8> = vec![];
        let start_bit = 4;
        let end_bit = 20;

        let result = bit_range(&data, start_bit, end_bit + 1);
        let expected: Vec<u8> = vec![];
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bit_range_out_of_range() {
        let data: Vec<u8> = vec![255];
        let start_bit = 4;
        let end_bit = 20;

        let result = bit_range(&data, start_bit, end_bit + 1);
        let expected: Vec<u8> = vec![0b00001111];
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitpos_1() {
        let key = "key";
        let value: Vec<u8> = vec![0xFF, 0xF0, 0x00];

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(move |_| Ok(Some(value.clone().into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(12))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["BITPOS".into(), key.into(), 0.to_string().into()];
        let _ = bitpos(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitpos_2() {
        let key = "key";
        let value = "foo";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(move |_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(1))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec!["BITPOS".into(), key.into(), 1.to_string().into()];
        let _ = bitpos(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitpos_start() {
        let key = "key";
        let value: Vec<u8> = vec![0xFF, 0xF0, 0x00];

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(move |_| Ok(Some(value.clone().into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(12))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITPOS".into(),
            key.into(),
            0.to_string().into(),
            1.to_string().into(),
        ];
        let _ = bitpos(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitpos_start_end_1() {
        let key = "key";
        let value: Vec<u8> = vec![0xFF, 0xF0, 0x00];

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(move |_| Ok(Some(value.clone().into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(12))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITPOS".into(),
            key.into(),
            0.to_string().into(),
            1.to_string().into(),
            3.to_string().into(),
        ];
        let _ = bitpos(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitpos_start_end_2() {
        let key = "key";
        let value = "foo";

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(move |_| Ok(Some(value.into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(-1))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITPOS".into(),
            key.into(),
            1.to_string().into(),
            10.to_string().into(),
            10.to_string().into(),
        ];
        let _ = bitpos(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitpos_start_end_3() {
        let key = "key";
        let value: Vec<u8> = vec![0b01100110, 0b01101111, 0b01101111];

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(move |_| Ok(Some(value.clone().into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(9))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITPOS".into(),
            key.into(),
            1.to_string().into(),
            1.to_string().into(),
            3.to_string().into(),
        ];
        let _ = bitpos(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_bitpos_bit_unit() {
        let key = "key";
        let value: Vec<u8> = vec![0xFF, 0xF0, 0x00];

        let mut mock_db = MockDatabaseOperations::new();
        mock_db
            .expect_get_string()
            .with(eq(key.as_bytes()))
            .times(1)
            .returning(move |_| Ok(Some(value.clone().into())));

        let mut mock_conn = MockConnection::new();
        mock_conn
            .expect_write_integer()
            .with(eq(-1))
            .times(1)
            .return_const(());

        let args: Vec<Vec<u8>> = vec![
            "BITPOS".into(),
            key.into(),
            0.to_string().into(),
            1.to_string().into(),
            3.to_string().into(),
            "BIT".into(),
        ];
        let _ = bitpos(&mut mock_conn, &mock_db, &args).unwrap();
    }

    #[test]
    fn test_find_first_bit_pos_byte_1() {
        assert_eq!(Some(4), find_first_bit_pos_byte(0xF0, 0));
    }

    #[test]
    fn test_find_first_bit_pos_byte_2() {
        assert_eq!(Some(0), find_first_bit_pos_byte(0x0F, 0));
    }

    #[test]
    fn test_find_first_bit_pos_byte_3() {
        assert_eq!(Some(1), find_first_bit_pos_byte(102, 1));
    }

    #[test]
    fn test_find_first_bit_pos_1() {
        assert_eq!(Some(7), find_first_bit_pos(&vec![1], 1, 0, 8));
    }

    #[test]
    fn test_find_first_bit_pos_2() {
        assert_eq!(Some(15), find_first_bit_pos(&vec![0, 1], 1, 0, 16));
    }

    #[test]
    fn test_find_first_bit_pos_3() {
        assert_eq!(Some(6), find_first_bit_pos(&vec![!2], 0, 0, 8));
    }

    #[test]
    fn test_find_first_bit_pos_4() {
        assert_eq!(
            Some(12),
            find_first_bit_pos(&vec![0xFF, 0xF0, 0x00], 0, 0, 24)
        );
    }

    #[test]
    fn test_find_first_bit_pos_5() {
        assert_eq!(None, find_first_bit_pos(&vec![0b01110000, 0, 0], 0, 1, 4));
    }

    #[test]
    fn test_find_first_bit_pos_bit_range_1() {
        let slice = bit_range(&vec![0xFF, 0xF0, 0x00], 1, 4);
        assert_eq!(vec![0b01110000, 0, 0], slice);
        assert_eq!(None, find_first_bit_pos(&slice, 0, 1, 4));
    }

    #[test]
    fn test_find_first_bit_pos_bit_range_2() {
        let slice = bit_range(&vec![0b01100110, 0b01101111, 0b01101111], 8, 24);
        assert_eq!(vec![0, 0b01101111, 0b01101111], slice);
        assert_eq!(Some(9), find_first_bit_pos(&slice, 1, 8, 24));
    }

    #[test]
    fn test_find_first_bit_pos_bit_range_3() {
        let slice = bit_range(&vec![0xFF, 0xF0, 0x00], 8, 24);
        assert_eq!(vec![0, 0xF0, 0], slice);
        assert_eq!(Some(12), find_first_bit_pos(&slice, 0, 8, 24));
    }

    #[test]
    fn test_find_first_bit_pos_empty() {
        assert_eq!(None, find_first_bit_pos(&vec![], 1, 0, 8));
    }
}
