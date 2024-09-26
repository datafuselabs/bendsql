// Copyright 2021 Datafuse Labs
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

use databend_driver::Client;

use crate::common::DEFAULT_DSN;

#[tokio::test]
async fn test_commit() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    conn.exec("CREATE OR REPLACE TABLE t(c int);")
        .await
        .unwrap();
    conn.begin().await.unwrap();
    conn.exec("INSERT INTO t VALUES(1);").await.unwrap();
    let row = conn.query_row("SELECT * FROM t").await.unwrap();
    let row = row.unwrap();
    let (val,): (i32,) = row.try_into().unwrap();
    assert_eq!(val, 1);
    conn.commit().await.unwrap();
    let row = conn.query_row("SELECT * FROM t").await.unwrap();
    let row = row.unwrap();
    let (val,): (i32,) = row.try_into().unwrap();
    assert_eq!(val, 1);
}

#[tokio::test]
async fn test_rollback() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    conn.exec("CREATE OR REPLACE TABLE t(c int);")
        .await
        .unwrap();
    conn.begin().await.unwrap();
    conn.exec("INSERT INTO t VALUES(1);").await.unwrap();
    let row = conn.query_row("SELECT * FROM t").await.unwrap();
    let row = row.unwrap();
    let (val,): (i32,) = row.try_into().unwrap();
    assert_eq!(val, 1);

    conn.rollback().await.unwrap();

    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();
    let row = conn.query_row("SELECT * FROM t").await.unwrap();
    assert!(row.is_none());
}
