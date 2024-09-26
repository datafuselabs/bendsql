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

use crate::error_code::ErrorCode;
use reqwest::StatusCode;

#[derive(Debug)]
pub enum Error {
    WithContext(Box<Error>, String),

    /// errors detected before sending request.
    /// e.g. invalid DSN, header value, stage name.
    BadArgument(String),
    /// errors when
    /// 1. accessing local file and presign_url
    /// 2. From(std::io::Error)
    IO(String),

    /// send request error
    Request(String),

    /// http handler return 200, but body is invalid
    /// 1. failed to decode body to Utf8 or JSON
    /// 2. failed to decode result data
    Decode(String),

    /// http handler return 200, but query failed (.error != null)
    QueryFailed(ErrorCode),

    /// http handler return non-200, with JSON body of type QueryError.
    Logic(StatusCode, ErrorCode),

    /// other non-200 response
    Response {
        status: StatusCode,
        msg: String,
    },

    /// the flowing are more detail type of Logic
    ///
    /// possible reasons:
    ///  - expired: if you have not polled the next_page_uri for too long, the session will be expired, you'll get a 404
    ///    on accessing this next page uri.
    ///  - routed to another server
    ///  - server restarted
    ///
    /// TODO: try to distinguish them
    QueryNotFound(String),
    AuthFailure(ErrorCode),
}

impl Error {
    pub fn response_error(status: StatusCode, body: &[u8]) -> Self {
        Self::Response {
            status,
            msg: String::from_utf8_lossy(body).to_string(),
        }
    }

    pub fn with_context(self, ctx: &str) -> Self {
        Error::WithContext(Box::new(self), ctx.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Decode(msg) => write!(f, "DecodeError: {msg}"),
            Error::BadArgument(msg) => write!(f, "BadArgument: {msg}"),
            Error::Request(msg) => write!(f, "{msg}"),
            Error::Response { msg, status } => write!(f, "ResponseError: ({status}){msg}"),
            Error::IO(msg) => write!(f, "IOError: {msg}"),
            Error::Logic(status_code, ec) => write!(f, "BadRequest:({status_code}){ec}"),
            Error::QueryNotFound(msg) => write!(f, "QueryNotFound: {msg}"),
            Error::QueryFailed(ec) => write!(f, "QueryFailed: {ec}"),
            Error::AuthFailure(ec) => write!(f, "AuthFailure: {ec}"),

            Error::WithContext(err, ctx) => write!(f, "fail to {ctx}: {err}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = core::result::Result<T, E>;

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::Decode(e.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error::Decode(e.to_string())
    }
}

/// only used in make_headers
impl From<reqwest::header::InvalidHeaderValue> for Error {
    fn from(e: reqwest::header::InvalidHeaderValue) -> Self {
        Error::BadArgument(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Decode(e.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Request(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Decode(e.to_string())
    }
}
