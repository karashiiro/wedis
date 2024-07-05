fn adjust_index(end_index: usize, x: i64) -> usize {
    let iend_index: i64 = end_index.try_into().unwrap();
    if x > iend_index {
        end_index
    } else if x >= 0 {
        x.try_into().unwrap()
    } else {
        // x < 0
        (iend_index + x + 1).try_into().unwrap()
    }
}

pub fn adjust_indices(end_index: usize, start: i64, end: i64) -> (usize, usize) {
    (adjust_index(end_index, start), adjust_index(end_index, end))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_adjust_indices_negative() {
        let end_index = 4;
        let start = -3;
        let end = -1;

        let (start, end) = adjust_indices(end_index, start, end);
        assert_eq!(2, start);
        assert_eq!(4, end);
    }
}
