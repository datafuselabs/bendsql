// Copyright 2023 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;

use serde::Deserialize;
use tokio_stream::Stream;

#[cfg(feature = "flight-sql")]
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::schema::SchemaRef;
use crate::value::Value;

pub type RowIterator = Pin<Box<dyn Stream<Item = Result<Row>> + Send>>;
pub type RowProgressIterator = Pin<Box<dyn Stream<Item = Result<RowWithProgress>> + Send>>;

#[derive(Clone, Debug)]
pub enum RowWithProgress {
    Row(Row),
    Progress(QueryProgress),
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct QueryProgress {
    #[serde(default)]
    pub total_rows: usize,
    #[serde(default)]
    pub total_bytes: usize,

    #[serde(default)]
    pub read_rows: usize,
    #[serde(default)]
    pub read_bytes: usize,

    #[serde(default)]
    pub write_rows: usize,
    #[serde(default)]
    pub write_bytes: usize,
}

impl QueryProgress {
    pub fn normalize(&mut self) {
        if self.total_rows == 0 {
            self.total_rows = self.read_rows;
        }
        if self.total_bytes == 0 {
            self.total_bytes = self.read_bytes;
        }
    }
}

impl From<databend_client::response::Progresses> for QueryProgress {
    fn from(progresses: databend_client::response::Progresses) -> Self {
        let mut p = Self {
            total_rows: 0,
            total_bytes: 0,
            read_rows: progresses.scan_progress.rows,
            read_bytes: progresses.scan_progress.bytes,
            write_rows: progresses.write_progress.rows,
            write_bytes: progresses.write_progress.bytes,
        };
        if let Some(total) = progresses.total_scan {
            p.total_rows = total.rows;
            p.total_bytes = total.bytes;
        }
        p
    }
}

#[derive(Clone, Debug, Default)]
pub struct Row(Vec<Value>);

impl TryFrom<(SchemaRef, &Vec<String>)> for Row {
    type Error = Error;

    fn try_from((schema, data): (SchemaRef, &Vec<String>)) -> Result<Self> {
        let mut values: Vec<Value> = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            values.push(Value::try_from((&field.data_type, data[i].as_str()))?);
        }
        Ok(Self(values))
    }
}

impl Row {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Rows(Vec<Row>);

#[cfg(feature = "flight-sql")]
impl TryFrom<RecordBatch> for Rows {
    type Error = Error;
    fn try_from(batch: RecordBatch) -> Result<Self> {
        let schema = batch.schema();
        let mut rows: Vec<Row> = Vec::new();
        for i in 0..batch.num_rows() {
            let mut values: Vec<Value> = Vec::new();
            for j in 0..schema.fields().len() {
                let v = batch.column(j);
                let field = schema.field(j);
                let value = Value::try_from((field, v, i))?;
                values.push(value);
            }
            rows.push(Row(values));
        }
        Ok(Self(rows))
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

// This macro implements TryFrom for tuple of types
macro_rules! impl_tuple_from_row {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> TryFrom<Row> for ($($Ti,)+)
        where
            $($Ti: TryFrom<Value>),+
        {
            type Error = Error;
            fn try_from(row: Row) -> Result<Self> {
                // It is not possible yet to get the number of metavariable repetitions
                // ref: https://github.com/rust-lang/lang-team/issues/28#issue-644523674
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);

                if expected_len != row.len() {
                    return Err(Error::InvalidResponse(format!("row size mismatch: expected {} columns, got {}", expected_len, row.len())));
                }
                let mut vals_iter = row.into_iter().enumerate();

                Ok((
                    $(
                        {
                            let (col_ix, col_value) = vals_iter
                                .next()
                                .unwrap(); // vals_iter size is checked before this code is reached,
                                           // so it is safe to unwrap
                            let t = col_value.get_type();
                            $Ti::try_from(col_value)
                                .map_err(|_| Error::InvalidResponse(format!("failed converting column {} from type({:?}) to type({})", col_ix, t, std::any::type_name::<$Ti>())))?
                        }
                    ,)+
                ))
            }
        }
    }
}

// Implement FromRow for tuples of size up to 16
impl_tuple_from_row!(T1);
impl_tuple_from_row!(T1, T2);
impl_tuple_from_row!(T1, T2, T3);
impl_tuple_from_row!(T1, T2, T3, T4);
impl_tuple_from_row!(T1, T2, T3, T4, T5);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
