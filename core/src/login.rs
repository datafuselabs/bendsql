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

use std::collections::BTreeMap;

use crate::request::SessionState;
use crate::response::QueryError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
pub struct LoginRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
}

impl From<&SessionState> for LoginRequest {
    fn from(value: &SessionState) -> Self {
        Self {
            database: value.database.clone(),
            role: value.role.clone(),
            settings: value.settings.clone(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct LoginInfo {
    pub version: String,
    pub session_id: String,
    pub session_token: String,
    pub session_token_validity_in_secs: u64,
    pub refresh_token: String,
    #[allow(dead_code)]
    pub refresh_token_validity_in_secs: u64,
}

impl LoginInfo {
    pub fn may_need_renew_token() {
    }
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum LoginResponse {
    Ok(LoginInfo),
    Err { error: QueryError },
}

#[derive(Serialize, Debug)]
pub struct RenewRequest {
    pub session_token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RenewInfo {
    pub session_token: String,
    pub session_token_validity_in_secs: u64,
    pub refresh_token: String,
    pub refresh_token_validity_in_secs: u64,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum RenewResponse {
    Ok(RenewInfo),
    Err { error: QueryError },
}
