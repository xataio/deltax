pub mod bitpacked;
pub mod boolean;
pub mod dictionary;
pub mod gorilla;
pub mod integer;
pub mod lz4;
pub mod numeric_scaled;

/// Tag byte identifying the compression codec used.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Gorilla = 1,
    DeltaVarint = 2,
    Dictionary = 3,
    Lz4 = 4,
    BooleanBitmap = 5,
    Lz4Blocked = 6,
    Constant = 7,
    ForBitpacked = 8,
    DictionaryLz4 = 9,
    // Binary variants of the string codecs: the payloads are raw bytes (a
    // type's binary representation), NOT UTF-8 text renderings. Emitted for
    // natively-coded fixed/varlena types (uuid, bytea, inet); the tag is what
    // distinguishes a new binary blob from a legacy text-rendering blob on
    // the same column, so mixed-generation partitions decode correctly.
    BinaryDictionary = 10,
    BinaryDictionaryLz4 = 11,
    BinaryLz4Blocked = 12,
    /// numeric stored as scaled i64 mantissas: data = [dscale u8][inner
    /// integer tag u8][inner encoding]. Written only when every non-null
    /// value in the segment has the same dscale and its mantissa fits i64
    /// (the typical `numeric(p,s)` column); otherwise the segment falls back
    /// to the text codecs — dispatch is per blob.
    NumericScaled = 13,
}

impl CompressionType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Gorilla,
            2 => Self::DeltaVarint,
            3 => Self::Dictionary,
            4 => Self::Lz4,
            5 => Self::BooleanBitmap,
            6 => Self::Lz4Blocked,
            7 => Self::Constant,
            8 => Self::ForBitpacked,
            9 => Self::DictionaryLz4,
            10 => Self::BinaryDictionary,
            11 => Self::BinaryDictionaryLz4,
            12 => Self::BinaryLz4Blocked,
            13 => Self::NumericScaled,
            _ => panic!("unknown compression type tag: {}", v),
        }
    }

    /// Remap a string-codec tag to its binary-payload variant. Used by
    /// `compress_binary_values`, which reuses the string codec pipeline
    /// byte-for-byte and only changes the tag so readers know the payloads
    /// are raw bytes rather than UTF-8 text.
    pub fn to_binary_variant(self) -> Self {
        match self {
            Self::Dictionary => Self::BinaryDictionary,
            Self::DictionaryLz4 => Self::BinaryDictionaryLz4,
            Self::Lz4Blocked => Self::BinaryLz4Blocked,
            other => other,
        }
    }
}

/// A compressed column blob with null handling.
///
/// Wire format:
///   1 byte  — CompressionType tag
///   4 bytes — total row count (including nulls), little-endian u32
///   1 byte  — has_nulls flag (0 or 1)
///   if has_nulls: ceil(row_count/8) bytes — null bitmap (bit=1 means null)
///   rest    — codec-specific compressed data (non-null values only)
pub struct CompressedColumn {
    pub type_tag: CompressionType,
    pub row_count: u32,
    pub null_bitmap: Vec<u8>,
    pub data: Vec<u8>,
}

impl CompressedColumn {
    pub fn to_bytes(&self) -> Vec<u8> {
        let has_nulls = !self.null_bitmap.is_empty();
        let bitmap_len = if has_nulls {
            (self.row_count as usize).div_ceil(8)
        } else {
            0
        };
        let total = 1 + 4 + 1 + bitmap_len + self.data.len();
        let mut buf = Vec::with_capacity(total);
        buf.push(self.type_tag as u8);
        buf.extend_from_slice(&self.row_count.to_le_bytes());
        buf.push(has_nulls as u8);
        if has_nulls {
            buf.extend_from_slice(&self.null_bitmap[..bitmap_len]);
        }
        buf.extend_from_slice(&self.data);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 6, "compressed column too short");
        let type_tag = CompressionType::from_u8(bytes[0]);
        let row_count = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        let has_nulls = bytes[5] != 0;
        let (null_bitmap, data_start) = if has_nulls {
            let bitmap_len = (row_count as usize).div_ceil(8);
            (bytes[6..6 + bitmap_len].to_vec(), 6 + bitmap_len)
        } else {
            (Vec::new(), 6)
        };
        let data = bytes[data_start..].to_vec();
        Self {
            type_tag,
            row_count,
            null_bitmap,
            data,
        }
    }
}

/// A borrowing view of a compressed column blob — avoids copying bitmap + data.
///
/// Same wire format as `CompressedColumn`, but references the original byte slice.
pub struct CompressedColumnRef<'a> {
    pub type_tag: CompressionType,
    pub row_count: u32,
    pub null_bitmap: &'a [u8],
    pub data: &'a [u8],
}

impl<'a> CompressedColumnRef<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        assert!(bytes.len() >= 6, "compressed column too short");
        let type_tag = CompressionType::from_u8(bytes[0]);
        let row_count = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        let has_nulls = bytes[5] != 0;
        let (null_bitmap, data_start) = if has_nulls {
            let bitmap_len = (row_count as usize).div_ceil(8);
            (&bytes[6..6 + bitmap_len], 6 + bitmap_len)
        } else {
            (&bytes[0..0], 6) // empty slice, no allocation
        };
        let data = &bytes[data_start..];
        Self {
            type_tag,
            row_count,
            null_bitmap,
            data,
        }
    }
}

/// Build a null bitmap from an iterator of Option values.
/// Returns (non_null_values, null_bitmap). Bitmap is empty if no nulls.
pub fn extract_nulls<T: Clone>(values: &[Option<T>]) -> (Vec<T>, Vec<u8>) {
    let mut non_null = Vec::with_capacity(values.len());
    let mut has_any_null = false;
    let bitmap_len = values.len().div_ceil(8);
    let mut bitmap = vec![0u8; bitmap_len];

    for (i, val) in values.iter().enumerate() {
        match val {
            Some(v) => non_null.push(v.clone()),
            None => {
                bitmap[i / 8] |= 1 << (i % 8);
                has_any_null = true;
            }
        }
    }

    if has_any_null {
        (non_null, bitmap)
    } else {
        (non_null, Vec::new())
    }
}

/// Re-insert nulls into a decompressed vector using the null bitmap.
pub fn reinsert_nulls<T: Default + Clone>(
    values: &[T],
    null_bitmap: &[u8],
    total_count: usize,
) -> Vec<Option<T>> {
    if null_bitmap.is_empty() {
        return values.iter().map(|v| Some(v.clone())).collect();
    }

    let mut result = Vec::with_capacity(total_count);
    let mut val_idx = 0;
    for i in 0..total_count {
        let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
        if is_null {
            result.push(None);
        } else {
            result.push(Some(values[val_idx].clone()));
            val_idx += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_column_roundtrip() {
        let cc = CompressedColumn {
            type_tag: CompressionType::Gorilla,
            row_count: 100,
            null_bitmap: Vec::new(),
            data: vec![1, 2, 3, 4, 5],
        };
        let bytes = cc.to_bytes();
        let cc2 = CompressedColumn::from_bytes(&bytes);
        assert_eq!(cc2.type_tag, CompressionType::Gorilla);
        assert_eq!(cc2.row_count, 100);
        assert!(cc2.null_bitmap.is_empty());
        assert_eq!(cc2.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_compressed_column_with_nulls() {
        let cc = CompressedColumn {
            type_tag: CompressionType::DeltaVarint,
            row_count: 16,
            null_bitmap: vec![0b00000101, 0b00000000], // nulls at index 0 and 2
            data: vec![10, 20, 30],
        };
        let bytes = cc.to_bytes();
        let cc2 = CompressedColumn::from_bytes(&bytes);
        assert_eq!(cc2.row_count, 16);
        assert_eq!(cc2.null_bitmap, vec![0b00000101, 0b00000000]);
        assert_eq!(cc2.data, vec![10, 20, 30]);
    }

    #[test]
    fn test_extract_and_reinsert_nulls() {
        let values: Vec<Option<i64>> = vec![Some(1), None, Some(3), None, Some(5)];
        let (non_null, bitmap) = extract_nulls(&values);
        assert_eq!(non_null, vec![1, 3, 5]);
        assert!(!bitmap.is_empty());

        let restored = reinsert_nulls(&non_null, &bitmap, 5);
        assert_eq!(restored, values);
    }

    #[test]
    fn test_extract_no_nulls() {
        let values: Vec<Option<i64>> = vec![Some(1), Some(2), Some(3)];
        let (non_null, bitmap) = extract_nulls(&values);
        assert_eq!(non_null, vec![1, 2, 3]);
        assert!(bitmap.is_empty());
    }
}
