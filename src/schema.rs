use std::{sync::Arc, vec};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema};

use crate::{Key, Value};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SemanticType {
    Tag,
    Timestamp,
    Field,
}

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub semantic: SemanticType,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<usize>,
    pub time_index: usize,
}

impl TableSchema {
    pub fn default_table() -> Self {
        Self {
            columns: vec![
                ColumnDef {
                    name: "tags".into(),
                    data_type: DataType::Binary,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "timestamp".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Timestamp,
                },
                ColumnDef {
                    name: "fields".into(),
                    data_type: DataType::Binary,
                    semantic: SemanticType::Field,
                },
            ],
            primary_key: vec![0],
            time_index: 1,
        }
    }

    pub fn arrow_schema(&self) -> Schema {
        Schema::new(
            self.columns
                .iter()
                .map(|c| {
                    Field::new(
                        &c.name,
                        c.data_type.clone(),
                        c.semantic == SemanticType::Field,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }

    fn push_len_perfixed(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }

    fn take_len_prefixed(data: &[u8], pos: &mut usize) -> Vec<u8> {
        let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;
        let v = data[*pos..*pos + len].to_vec();
        *pos += len;
        v
    }

    pub fn encode_tags(&self, cells: &[Vec<u8>]) -> Vec<u8> {
        if self.primary_key.len() == 1 {
            cells[self.primary_key[0]].clone()
        } else {
            let mut out = Vec::new();
            for &idx in &self.primary_key {
                Self::push_len_perfixed(&mut out, &cells[idx]);
            }
            out
        }
    }

    pub fn decode_tags(&self, bytes: &[u8]) -> Vec<Vec<u8>> {
        if self.primary_key.len() == 1 {
            vec![bytes.to_vec()]
        } else {
            let mut out = Vec::with_capacity(self.primary_key.len());
            let mut pos = 0;
            for _ in &self.primary_key {
                out.push(Self::take_len_prefixed(bytes, &mut pos));
            }
            out
        }
    }

    pub fn encode_fields(&self, cells: &[Vec<u8>]) -> Vec<u8> {
        let mut first: Option<usize> = None;
        let mut count = 0;
        for (i, c) in self.columns.iter().enumerate() {
            if c.semantic == SemanticType::Field {
                if first.is_none() {
                    first = Some(i);
                }
                count += 1;
            }
        }
        if count == 1 {
            return cells[first.unwrap()].clone();
        }
        let mut out = Vec::new();
        for (i, c) in self.columns.iter().enumerate() {
            if c.semantic == SemanticType::Field {
                Self::push_len_perfixed(&mut out, &cells[i]);
            }
        }
        out
    }

    pub fn decode_fields(&self, bytes: &[u8]) -> Vec<Vec<u8>> {
        let n = self
            .columns
            .iter()
            .filter(|c| c.semantic == SemanticType::Field)
            .count();
        if n == 1 {
            vec![bytes.to_vec()]
        } else {
            let mut out = Vec::with_capacity(n);
            let mut pos = 0;
            for _ in 0..n {
                out.push(Self::take_len_prefixed(bytes, &mut pos));
            }
            out
        }
    }

    pub fn key_to_cells(&self, key: &Key) -> Vec<Vec<u8>> {
        let tags = self.decode_tags(&key.0);
        let mut cells = vec![Vec::new(); self.columns.len()];
        for (j, &idx) in self.primary_key.iter().enumerate() {
            cells[idx] = tags[j].clone();
        }
        cells[self.time_index] = key.1.to_le_bytes().to_vec();
        cells
    }

    pub fn cells_to_key(&self, cells: &[Vec<u8>]) -> Key {
        let tags = self.encode_tags(cells);
        let ts = i64::from_le_bytes(cells[self.time_index].as_slice().try_into().unwrap());
        (tags, ts)
    }

    pub fn key(&self, tags: &[Vec<u8>], ts: i64) -> Key {
        let mut cells = vec![Vec::new(); self.columns.len()];
        for (j, &idx) in self.primary_key.iter().enumerate() {
            cells[idx] = tags[j].clone();
        }
        cells[self.time_index] = ts.to_le_bytes().to_vec();
        self.cells_to_key(&cells)
    }

    pub fn value(&self, fields: &[Vec<u8>]) -> Value {
        let mut cells = vec![Vec::new(); self.columns.len()];
        let mut k = 0;
        for (i, c) in self.columns.iter().enumerate() {
            if c.semantic == SemanticType::Field {
                cells[i] = fields[k].clone();
                k += 1;
            }
        }
        self.encode_fields(&cells)
    }
}

pub(crate) fn parse_column_cells(dt: &DataType, arr: &ArrayRef, n: usize) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let cell = match dt {
            DataType::Binary => {
                let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
                (!a.is_null(i)).then(|| a.value(i).to_vec())
            }
            DataType::Utf8 => {
                let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
                (!a.is_null(i)).then(|| a.value(i).as_bytes().to_vec())
            }
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                (!a.is_null(i)).then(|| a.value(i).to_le_bytes().to_vec())
            }
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                (!a.is_null(i)).then(|| a.value(i).to_le_bytes().to_vec())
            }
            DataType::Boolean => {
                let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                (!a.is_null(i)).then(|| vec![a.value(i) as u8])
            }
            DataType::Timestamp(..) => {
                let a = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                (!a.is_null(i)).then(|| a.value(i).to_le_bytes().to_vec())
            }
            other => unimplemented!("cell encoding for {other:?}"),
        };
        out.push(cell);
    }
    out
}

pub(crate) fn cells_to_array(dt: &DataType, rows: &[Option<Vec<u8>>]) -> ArrayRef {
    match dt {
        DataType::Binary => Arc::new(BinaryArray::from_iter(rows.iter().map(|o| o.as_deref()))),
        DataType::Utf8 => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|o| o.as_deref().map(|b| std::str::from_utf8(b).unwrap())),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter(rows.iter().map(|o| {
            o.as_ref()
                .map(|b| i64::from_le_bytes(b.as_slice().try_into().unwrap()))
        }))),
        DataType::Float64 => Arc::new(Float64Array::from_iter(rows.iter().map(|o| {
            o.as_ref()
                .map(|b| f64::from_le_bytes(b.as_slice().try_into().unwrap()))
        }))),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(
            rows.iter().map(|o| o.as_ref().map(|b| b[0] != 0)),
        )),
        DataType::Timestamp(..) => {
            Arc::new(TimestampNanosecondArray::from_iter(rows.iter().map(|o| {
                o.as_ref()
                    .map(|b| i64::from_le_bytes(b.as_slice().try_into().unwrap()))
            })))
        }
        other => unimplemented!("array build for {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn sample_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnDef {
                    name: "host".into(),
                    data_type: DataType::Binary,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "region".into(),
                    data_type: DataType::Binary,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "ts".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Timestamp,
                },
                ColumnDef {
                    name: "cpu".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Field,
                },
                ColumnDef {
                    name: "mem".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Field,
                },
            ],
            primary_key: vec![0, 1],
            time_index: 2,
        }
    }

    #[test]
    fn test_key_value_helpers() {
        let s = TableSchema::default_table();
        assert_eq!(s.key(&[vec![7]], 42), (vec![7], 42));
        assert_eq!(s.value(&[b"abc".to_vec()]), b"abc".to_vec());
    }

    #[test]
    fn test_single_tag_single_field_identity() {
        let s = TableSchema::default_table();
        let tag: Vec<u8> = vec![10, 20];
        let ts: i64 = 1234;
        let field: Vec<u8> = b"hello".to_vec();
        let cells = vec![tag.clone(), ts.to_le_bytes().to_vec(), field.clone()];

        let key = s.cells_to_key(&cells);
        assert_eq!(key, (tag.clone(), ts));

        let cells_back = s.key_to_cells(&key);
        assert_eq!(cells_back[0], tag);
        assert_eq!(cells_back[1], ts.to_le_bytes().to_vec());

        let blob = s.encode_fields(&cells);
        assert_eq!(blob, field);
        assert_eq!(s.decode_fields(&blob), vec![field]);
    }

    #[test]
    fn test_multi_tag_multi_field_still_prefixed() {
        let s = sample_schema();
        let cells = vec![
            b"h1".to_vec(),
            b"cn".to_vec(),
            0i64.to_le_bytes().to_vec(),
            1i64.to_le_bytes().to_vec(),
            2i64.to_le_bytes().to_vec(),
        ];
        let key = s.cells_to_key(&cells);
        assert_eq!(key.0.len(), (4 + b"h1".len()) + (4 + b"cn".len()));

        let cells_back = s.key_to_cells(&key);
        assert_eq!(cells_back[0], b"h1".to_vec());
        assert_eq!(cells_back[1], b"cn".to_vec());
        assert_eq!(cells_back[2], 0i64.to_le_bytes().to_vec());

        let blob = s.encode_fields(&cells);
        assert_eq!(
            s.decode_fields(&blob),
            vec![1i64.to_le_bytes().to_vec(), 2i64.to_le_bytes().to_vec()]
        );
    }

    #[test]
    fn test_tags_roundtrip() {
        let s = sample_schema();
        let cells = vec![b"h1".to_vec(), b"cn".to_vec()];
        let enc = s.encode_tags(&cells);
        assert_eq!(s.decode_tags(&enc), cells);
    }

    #[test]
    fn test_fields_roundtrip() {
        let s = sample_schema();
        let cells = vec![
            b"h1".to_vec(),
            b"cn".to_vec(),
            0i64.to_le_bytes().to_vec(),
            1i64.to_le_bytes().to_vec(),
            2i64.to_le_bytes().to_vec(),
        ];
        let enc = s.encode_fields(&cells);
        assert_eq!(
            s.decode_fields(&enc),
            vec![1i64.to_le_bytes().to_vec(), 2i64.to_le_bytes().to_vec(),]
        );
    }
}
