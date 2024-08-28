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
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::auth::{AccessTokenAuth, AccessTokenFileAuth, Auth, BasicAuth};
use crate::error_code::{need_renew_token, ResponseWithErrorCode};
use crate::login::{
    LoginInfo, LoginRequest, LoginResponse, LogoutRequest, RenewRequest, RenewResponse,
};
use crate::presign::{presign_upload_to_stage, PresignMode, PresignedResponse, Reader};
use crate::stage::StageLocation;
use crate::{
    error::{Error, Result},
    request::{PaginationConfig, QueryRequest, SessionState, StageAttachmentConfig},
    response::QueryResponse,
};
use log::{info, warn};
use once_cell::sync::Lazy;
use percent_encoding::percent_decode_str;
use reqwest::header::HeaderMap;
use reqwest::multipart::{Form, Part};
use reqwest::{Body, Client as HttpClient, Request, RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_retry::strategy::jitter;
use tokio_util::io::ReaderStream;
use url::Url;

const HEADER_QUERY_ID: &str = "X-DATABEND-QUERY-ID";
const HEADER_TENANT: &str = "X-DATABEND-TENANT";
const HEADER_WAREHOUSE: &str = "X-DATABEND-WAREHOUSE";
const HEADER_STAGE_NAME: &str = "X-DATABEND-STAGE-NAME";
const HEADER_ROUTE_HINT: &str = "X-DATABEND-ROUTE-HINT";
const TXN_STATE_ACTIVE: &str = "Active";

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

#[derive(Clone)]
pub struct APIClient {
    pub cli: HttpClient,
    pub scheme: String,
    pub host: String,
    pub port: u16,

    endpoint: Url,

    auth: Arc<dyn Auth>,

    tenant: Option<String>,
    warehouse: Arc<parking_lot::Mutex<Option<String>>>,
    session_state: Arc<Mutex<SessionState>>,
    route_hint: Arc<RouteHintGenerator>,

    disable_session_token: bool,
    login_info: Option<Arc<parking_lot::Mutex<(LoginInfo, Instant)>>>,

    wait_time_secs: Option<i64>,
    max_rows_in_buffer: Option<i64>,
    max_rows_per_page: Option<i64>,

    connect_timeout: Duration,
    page_request_timeout: Duration,

    tls_ca_file: Option<String>,

    presign: PresignMode,
}

impl APIClient {
    pub async fn new(dsn: &str, name: Option<String>) -> Result<Self> {
        let mut client = Self::from_dsn(dsn).await?;
        client.build_client(name).await?;
        client.check_presign().await?;
        if !client.disable_session_token {
            client.login().await?;
        }
        Ok(client)
    }

    async fn from_dsn(dsn: &str) -> Result<Self> {
        let u = Url::parse(dsn)?;
        let mut client = Self::default();
        if let Some(host) = u.host_str() {
            client.host = host.to_string();
        }

        if u.username() != "" {
            let password = u.password().unwrap_or_default();
            let password = percent_decode_str(password).decode_utf8()?;
            client.auth = Arc::new(BasicAuth::new(u.username(), password));
        }
        let database = match u.path().trim_start_matches('/') {
            "" => None,
            s => Some(s.to_string()),
        };
        let mut role = None;
        let mut scheme = "https";
        let mut session_settings = BTreeMap::new();
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "wait_time_secs" => {
                    client.wait_time_secs = Some(v.parse()?);
                }
                "max_rows_in_buffer" => {
                    client.max_rows_in_buffer = Some(v.parse()?);
                }
                "max_rows_per_page" => {
                    client.max_rows_per_page = Some(v.parse()?);
                }
                "connect_timeout" => client.connect_timeout = Duration::from_secs(v.parse()?),
                "page_request_timeout_secs" => {
                    client.page_request_timeout = {
                        let secs: u64 = v.parse()?;
                        Duration::from_secs(secs)
                    };
                }
                "presign" => {
                    client.presign = match v.as_ref() {
                        "auto" => PresignMode::Auto,
                        "detect" => PresignMode::Detect,
                        "on" => PresignMode::On,
                        "off" => PresignMode::Off,
                        _ => {
                            return Err(Error::BadArgument(format!(
                            "Invalid value for presign: {}, should be one of auto/detect/on/off",
                            v
                        )))
                        }
                    }
                }
                "tenant" => {
                    client.tenant = Some(v.to_string());
                }
                "warehouse" => {
                    client.warehouse = Arc::new(parking_lot::Mutex::new(Some(v.to_string())));
                }
                "role" => role = Some(v.to_string()),
                "sslmode" => match v.as_ref() {
                    "disable" => scheme = "http",
                    "require" | "enable" => scheme = "https",
                    _ => {
                        return Err(Error::BadArgument(format!(
                            "Invalid value for sslmode: {}",
                            v
                        )))
                    }
                },
                "tls_ca_file" => {
                    client.tls_ca_file = Some(v.to_string());
                }
                "access_token" => {
                    client.auth = Arc::new(AccessTokenAuth::new(v));
                }
                "access_token_file" => {
                    client.auth = Arc::new(AccessTokenFileAuth::new(v));
                }
                "session_token" => {
                    client.disable_session_token = match v.as_ref() {
                        "disable" => true,
                        "enable" => false,
                        _ => {
                            return Err(Error::BadArgument(format!(
                                "Invalid value for session_token: {}",
                                v
                            )))
                        }
                    }
                }
                _ => {
                    session_settings.insert(k.to_string(), v.to_string());
                }
            }
        }
        client.port = match u.port() {
            Some(p) => p,
            None => match scheme {
                "http" => 80,
                "https" => 443,
                _ => unreachable!(),
            },
        };
        client.scheme = scheme.to_string();

        client.endpoint = Url::parse(&format!("{}://{}:{}", scheme, client.host, client.port))?;
        client.session_state = Arc::new(Mutex::new(
            SessionState::default()
                .with_settings(Some(session_settings))
                .with_role(role)
                .with_database(database),
        ));

        Ok(client)
    }

    async fn build_client(&mut self, name: Option<String>) -> Result<()> {
        let ua = match name {
            Some(n) => n,
            None => format!("databend-client-rust/{}", VERSION.as_str()),
        };
        let mut cli_builder = HttpClient::builder()
            .user_agent(ua)
            .pool_idle_timeout(Duration::from_secs(1));
        #[cfg(any(feature = "rustls", feature = "native-tls"))]
        if self.scheme == "https" {
            if let Some(ref ca_file) = self.tls_ca_file {
                let cert_pem = tokio::fs::read(ca_file).await?;
                let cert = reqwest::Certificate::from_pem(&cert_pem)?;
                cli_builder = cli_builder.add_root_certificate(cert);
            }
        }
        self.cli = cli_builder.build()?;
        Ok(())
    }

    async fn check_presign(&mut self) -> Result<()> {
        match self.presign {
            PresignMode::Auto => {
                if self.host.ends_with(".databend.com") || self.host.ends_with(".databend.cn") {
                    self.presign = PresignMode::On;
                } else {
                    self.presign = PresignMode::Off;
                }
            }
            PresignMode::Detect => match self.get_presigned_upload_url("@~/.bendsql/check").await {
                Ok(_) => self.presign = PresignMode::On,
                Err(e) => {
                    warn!("presign mode off with error detected: {}", e);
                    self.presign = PresignMode::Off;
                }
            },
            _ => {}
        }
        Ok(())
    }

    pub async fn current_warehouse(&self) -> Option<String> {
        let guard = self.warehouse.lock();
        guard.clone()
    }

    pub async fn current_database(&self) -> Option<String> {
        let guard = self.session_state.lock().await;
        guard.database.clone()
    }

    pub async fn current_role(&self) -> Option<String> {
        let guard = self.session_state.lock().await;
        guard.role.clone()
    }

    async fn in_active_transaction(&self) -> bool {
        let guard = self.session_state.lock().await;
        guard
            .txn_state
            .as_ref()
            .map(|s| s.eq_ignore_ascii_case(TXN_STATE_ACTIVE))
            .unwrap_or(false)
    }

    pub fn username(&self) -> String {
        self.auth.username()
    }

    fn gen_query_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    pub async fn handle_session(&self, session: &Option<SessionState>) {
        let session = match session {
            Some(session) => session,
            None => return,
        };

        // save the updated session state from the server side
        {
            let mut session_state = self.session_state.lock().await;
            *session_state = session.clone();
        }

        // process warehouse changed via session settings
        if let Some(settings) = session.settings.as_ref() {
            if let Some(v) = settings.get("warehouse") {
                let mut warehouse = self.warehouse.lock();
                *warehouse = Some(v.clone());
            }
        }
    }

    pub fn handle_warnings(&self, resp: &QueryResponse) {
        if let Some(warnings) = &resp.warnings {
            for w in warnings {
                warn!(target: "server_warnings", "server warning: {}", w);
            }
        }
    }

    pub async fn start_query(&self, sql: &str) -> Result<QueryResponse> {
        self.start_query_inner(sql, None).await
    }

    async fn wrap_auth_or_session_token(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        if let Some(info) = &self.login_info {
            let info = info.lock();
            Ok(builder.bearer_auth(info.0.session_token.clone()))
        } else {
            self.auth.wrap(builder).await
        }
    }

    async fn start_query_inner(
        &self,
        sql: &str,
        stage_attachment_config: Option<StageAttachmentConfig<'_>>,
    ) -> Result<QueryResponse> {
        if !self.in_active_transaction().await {
            self.route_hint.next();
        }
        let endpoint = self.endpoint.join("v1/query")?;

        // body
        let session_state = self.session_state().await;
        let req = QueryRequest::new(sql)
            .with_pagination(self.make_pagination())
            .with_session(Some(session_state))
            .with_stage_attachment(stage_attachment_config);

        // headers
        let query_id = self.gen_query_id();
        let headers = self.make_headers(Some(&query_id))?;

        let mut builder = self.cli.post(endpoint.clone()).json(&req);
        builder = self.wrap_auth_or_session_token(builder).await?;
        let request = builder.headers(headers.clone()).build()?;
        let response = self.query_request_helper(request, true, true).await?;
        if let Some(route_hint) = response.headers().get(HEADER_ROUTE_HINT) {
            self.route_hint.set(route_hint.to_str().unwrap_or_default());
        }
        let body = response.bytes().await?;
        let result: QueryResponse = json_from_slice(&body)?;
        self.handle_session(&result.session).await;
        if let Some(err) = result.error {
            return Err(Error::QueryFailed(err));
        }
        self.handle_warnings(&result);
        Ok(result)
    }

    pub async fn query_page(&self, query_id: &str, next_uri: &str) -> Result<QueryResponse> {
        info!("query page: {}", next_uri);
        let endpoint = self.endpoint.join(next_uri)?;
        let headers = self.make_headers(Some(query_id))?;
        let mut builder = self.cli.get(endpoint.clone());
        builder = self.wrap_auth_or_session_token(builder).await?;
        let request = builder
            .headers(headers.clone())
            .timeout(self.page_request_timeout)
            .build()?;

        let response = self.query_request_helper(request, false, true).await?;
        let body = response.bytes().await?;
        let resp: QueryResponse = json_from_slice(&body).map_err(|e| {
            if let Error::Logic(status, ec) = &e {
                if *status == 404 {
                    return Error::QueryNotFound(ec.message.clone());
                }
            }
            e
        })?;
        self.handle_session(&resp.session).await;
        match resp.error {
            Some(err) => Err(Error::QueryFailed(err)),
            None => Ok(resp),
        }
    }

    pub async fn kill_query(&self, query_id: &str, kill_uri: &str) -> Result<()> {
        info!("kill query: {}", kill_uri);
        let endpoint = self.endpoint.join(kill_uri)?;
        let headers = self.make_headers(Some(query_id))?;
        let mut builder = self.cli.post(endpoint.clone());
        builder = self.wrap_auth_or_session_token(builder).await?;
        let resp = builder.headers(headers.clone()).send().await?;
        if resp.status() != 200 {
            return Err(Error::response_error(resp.status(), &resp.bytes().await?)
                .with_context("kill query"));
        }
        Ok(())
    }

    pub async fn wait_for_query(&self, resp: QueryResponse) -> Result<QueryResponse> {
        info!("wait for query: {}", resp.id);
        if let Some(next_uri) = &resp.next_uri {
            let schema = resp.schema;
            let mut data = resp.data;
            let mut resp = self.query_page(&resp.id, next_uri).await?;
            while let Some(next_uri) = &resp.next_uri {
                resp = self.query_page(&resp.id, next_uri).await?;
                data.append(&mut resp.data);
            }
            resp.schema = schema;
            resp.data = data;
            Ok(resp)
        } else {
            Ok(resp)
        }
    }

    pub async fn query(&self, sql: &str) -> Result<QueryResponse> {
        info!("query: {}", sql);
        let resp = self.start_query(sql).await?;
        self.wait_for_query(resp).await
    }

    async fn session_state(&self) -> SessionState {
        self.session_state.lock().await.clone()
    }

    fn make_pagination(&self) -> Option<PaginationConfig> {
        if self.wait_time_secs.is_none()
            && self.max_rows_in_buffer.is_none()
            && self.max_rows_per_page.is_none()
        {
            return None;
        }
        let mut pagination = PaginationConfig {
            wait_time_secs: None,
            max_rows_in_buffer: None,
            max_rows_per_page: None,
        };
        if let Some(wait_time_secs) = self.wait_time_secs {
            pagination.wait_time_secs = Some(wait_time_secs);
        }
        if let Some(max_rows_in_buffer) = self.max_rows_in_buffer {
            pagination.max_rows_in_buffer = Some(max_rows_in_buffer);
        }
        if let Some(max_rows_per_page) = self.max_rows_per_page {
            pagination.max_rows_per_page = Some(max_rows_per_page);
        }
        Some(pagination)
    }

    fn make_headers(&self, query_id: Option<&str>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(tenant) = &self.tenant {
            headers.insert(HEADER_TENANT, tenant.parse()?);
        }
        let warehouse = self.warehouse.lock().clone();
        if let Some(warehouse) = warehouse {
            headers.insert(HEADER_WAREHOUSE, warehouse.parse()?);
        }
        let route_hint = self.route_hint.current();
        headers.insert(HEADER_ROUTE_HINT, route_hint.parse()?);
        if let Some(query_id) = query_id {
            headers.insert(HEADER_QUERY_ID, query_id.parse()?);
        }
        Ok(headers)
    }

    pub async fn insert_with_stage(
        &self,
        sql: &str,
        stage: &str,
        file_format_options: BTreeMap<&str, &str>,
        copy_options: BTreeMap<&str, &str>,
    ) -> Result<QueryResponse> {
        info!(
            "insert with stage: {}, format: {:?}, copy: {:?}",
            sql, file_format_options, copy_options
        );
        let stage_attachment = Some(StageAttachmentConfig {
            location: stage,
            file_format_options: Some(file_format_options),
            copy_options: Some(copy_options),
        });
        let resp = self.start_query_inner(sql, stage_attachment).await?;
        let resp = self.wait_for_query(resp).await?;
        Ok(resp)
    }

    async fn get_presigned_upload_url(&self, stage: &str) -> Result<PresignedResponse> {
        info!("get presigned upload url: {}", stage);
        let sql = format!("PRESIGN UPLOAD {}", stage);
        let resp = self.query(&sql).await?;
        if resp.data.len() != 1 {
            return Err(Error::Decode(
                "Empty response from server for presigned request".to_string(),
            ));
        }
        if resp.data[0].len() != 3 {
            return Err(Error::Decode(
                "Invalid response from server for presigned request".to_string(),
            ));
        }
        // resp.data[0]: [ "PUT", "{\"host\":\"s3.us-east-2.amazonaws.com\"}", "https://s3.us-east-2.amazonaws.com/query-storage-xxxxx/tnxxxxx/stage/user/xxxx/xxx?" ]
        let method = resp.data[0][0].clone().unwrap_or_default();
        if method != "PUT" {
            return Err(Error::Decode(format!(
                "Invalid method for presigned upload request: {}",
                method
            )));
        }
        let headers: BTreeMap<String, String> =
            serde_json::from_str(resp.data[0][1].clone().unwrap_or("{}".to_string()).as_str())?;
        let url = resp.data[0][2].clone().unwrap_or_default();
        Ok(PresignedResponse {
            method,
            headers,
            url,
        })
    }

    pub async fn upload_to_stage(&self, stage: &str, data: Reader, size: u64) -> Result<()> {
        match self.presign {
            PresignMode::Off => self.upload_to_stage_with_stream(stage, data, size).await,
            PresignMode::On => {
                let presigned = self.get_presigned_upload_url(stage).await?;
                presign_upload_to_stage(presigned, data, size).await
            }
            PresignMode::Auto => {
                unreachable!("PresignMode::Auto should be handled during client initialization")
            }
            PresignMode::Detect => {
                unreachable!("PresignMode::Detect should be handled during client initialization")
            }
        }
    }

    /// Upload data to stage with stream api, should not be used directly, use `upload_to_stage` instead.
    async fn upload_to_stage_with_stream(
        &self,
        stage: &str,
        data: Reader,
        size: u64,
    ) -> Result<()> {
        info!("upload to stage with stream: {}, size: {}", stage, size);
        if let Some(info) = self.need_pre_renew_session().await {
            self.renew_token(info).await?;
        }
        let endpoint = self.endpoint.join("v1/upload_to_stage")?;
        let location = StageLocation::try_from(stage)?;
        let query_id = self.gen_query_id();
        let mut headers = self.make_headers(Some(&query_id))?;
        headers.insert(HEADER_STAGE_NAME, location.name.parse()?);
        let stream = Body::wrap_stream(ReaderStream::new(data));
        let part = Part::stream_with_length(stream, size).file_name(location.path);
        let form = Form::new().part("upload", part);
        let mut builder = self.cli.put(endpoint.clone());
        builder = self.wrap_auth_or_session_token(builder).await?;
        let resp = builder.headers(headers).multipart(form).send().await?;
        let status = resp.status();
        if status != 200 {
            return Err(
                Error::response_error(status, &resp.bytes().await?).with_context("upload_to_stage")
            );
        }
        Ok(())
    }

    pub async fn login(&mut self) -> Result<()> {
        let endpoint = self.endpoint.join("/v1/session/login")?;
        let headers = self.make_headers(None)?;
        let body = LoginRequest::from(&*self.session_state.lock().await);
        let builder = self.cli.post(endpoint.clone()).json(&body);
        let builder = self.auth.wrap(builder).await?;
        let request = builder
            .headers(headers.clone())
            .timeout(self.connect_timeout)
            .build()?;
        let response = self.query_request_helper(request, true, false).await;
        let response = match response {
            Ok(r) => r,
            Err(Error::Logic(status, _ec)) if status == 404 => {
                // old server
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let body = response.bytes().await?;
        let response = json_from_slice(&body)?;
        match response {
            LoginResponse::Err { error } => return Err(Error::AuthFailure(error)),
            LoginResponse::Ok(info) => {
                self.login_info = Some(Arc::new(parking_lot::Mutex::new((info, Instant::now()))))
            }
        }
        Ok(())
    }

    pub fn build_log_out_request(&self, login_info: LoginInfo) -> Result<Request> {
        let endpoint = self.endpoint.join("/v1/session/logout")?;
        let body = LogoutRequest {
            refresh_token: login_info.refresh_token.clone(),
        };
        let headers = self.make_headers(None)?;
        let req = self
            .cli
            .post(endpoint.clone())
            .json(&body)
            .headers(headers.clone())
            .bearer_auth(login_info.session_token.clone())
            .build()?;
        Ok(req)
    }

    pub async fn close(&mut self) -> Result<()> {
        if let Some(info) = mem::take(&mut self.login_info) {
            let req = self.build_log_out_request(info.lock().0.clone())?;
            self.cli.execute(req).await?;
        }
        Ok(())
    }

    pub async fn renew_token(
        &self,
        self_login_info: Arc<parking_lot::Mutex<(LoginInfo, Instant)>>,
    ) -> Result<()> {
        let login_info = { self_login_info.lock().clone() };
        let endpoint = self.endpoint.join("/v1/session/renew")?;
        let body = RenewRequest {
            session_token: login_info.0.session_token.clone(),
        };
        let headers = self.make_headers(None)?;
        let request = self
            .cli
            .post(endpoint.clone())
            .json(&body)
            .headers(headers.clone())
            .bearer_auth(login_info.0.refresh_token.clone())
            .timeout(self.connect_timeout)
            .build()?;

        // avoid recursively call request_helper
        for i in 0..3 {
            let req = request.try_clone().expect("request not cloneable");
            match self.cli.execute(req).await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.bytes().await?;
                    if status == StatusCode::OK {
                        let response = json_from_slice(&body)?;
                        return match response {
                            RenewResponse::Err { error } => Err(Error::AuthFailure(error)),
                            RenewResponse::Ok(info) => {
                                *self_login_info.lock() = (
                                    LoginInfo {
                                        session_token: info.session_token,
                                        refresh_token: info.refresh_token,
                                        refresh_token_validity_in_secs: info
                                            .refresh_token_validity_in_secs,
                                        session_token_validity_in_secs: info
                                            .session_token_validity_in_secs,
                                        ..login_info.0.clone()
                                    },
                                    Instant::now(),
                                );
                                Ok(())
                            }
                        };
                    }
                    if status != StatusCode::SERVICE_UNAVAILABLE || i >= 2 {
                        return Err(Error::response_error(status, &body));
                    }
                }
                Err(err) => {
                    if !(err.is_timeout() || err.is_connect()) || i > 2 {
                        return Err(Error::Request(err.to_string()));
                    }
                }
            };
            sleep(jitter(Duration::from_secs(10))).await;
        }
        Ok(())
    }

    async fn need_pre_renew_session(
        &self,
    ) -> Option<Arc<parking_lot::Mutex<(LoginInfo, Instant)>>> {
        if let Some(login_info) = &self.login_info {
            let (start, ttl) = {
                let guard = login_info.lock();
                (guard.1, guard.0.session_token_validity_in_secs)
            };
            if Instant::now() > start + Duration::from_secs(ttl) {
                return Some(login_info.clone());
            }
        }
        None
    }

    /// return Ok if and only if status code is 200.
    ///
    /// retry on
    ///   - network errors
    ///   - (optional) 503
    ///
    /// renew databend token or reload jwt token if needed.
    async fn query_request_helper(
        &self,
        mut request: Request,
        retry_if_503: bool,
        renew_if_401: bool,
    ) -> std::result::Result<Response, Error> {
        let mut renewed = false;
        let mut retries = 0;
        loop {
            let req = request.try_clone().expect("request not cloneable");
            let (err, retry): (Error, bool) = match self.cli.execute(req).await {
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::OK {
                        return Ok(response);
                    }
                    let body = response.bytes().await?;
                    if retry_if_503 || status == StatusCode::SERVICE_UNAVAILABLE {
                        // waiting for server to start
                        (Error::response_error(status, &body), true)
                    } else {
                        let resp = serde_json::from_slice::<ResponseWithErrorCode>(&body);
                        match resp {
                            Ok(r) => {
                                let e = r.error;
                                if status == StatusCode::UNAUTHORIZED {
                                    request.headers_mut().remove(reqwest::header::AUTHORIZATION);
                                    if let Some(login_info) = &self.login_info {
                                        info!(
                                            "will retry {} after renew token on auth error {}",
                                            request.url(),
                                            e
                                        );
                                        let retry =
                                            if need_renew_token(e.code) && !renewed && renew_if_401
                                            {
                                                self.renew_token(login_info.clone()).await?;
                                                renewed = true;
                                                true
                                            } else {
                                                false
                                            };
                                        (Error::AuthFailure(e), retry)
                                    } else if self.auth.can_reload() {
                                        info!(
                                            "will retry {} after reload token on auth error {}",
                                            request.url(),
                                            e
                                        );
                                        let builder = RequestBuilder::from_parts(
                                            HttpClient::new(),
                                            request.try_clone().unwrap(),
                                        );
                                        let builder = self.auth.wrap(builder).await?;
                                        request = builder.build()?;
                                        (Error::AuthFailure(e), true)
                                    } else {
                                        (Error::AuthFailure(e), false)
                                    }
                                } else {
                                    (Error::Logic(status, e), false)
                                }
                            }
                            Err(_) => (
                                Error::Response {
                                    status,
                                    msg: String::from_utf8_lossy(&body).to_string(),
                                },
                                true,
                            ),
                        }
                    }
                }
                Err(err) => (
                    Error::Request(err.to_string()),
                    err.is_timeout() || err.is_connect(),
                ),
            };
            if !retry {
                return Err(err.with_context(&format!(
                    "fail to {} {} after 3 reties",
                    request.method(),
                    request.url()
                )));
            }
            match &err {
                Error::AuthFailure(_) => {
                    if renewed {
                        retries = 0;
                    } else if retries == 2 {
                        return Err(err.with_context(&format!(
                            "fail to {} {} after 3 reties",
                            request.method(),
                            request.url()
                        )));
                    }
                }
                _ => {
                    if retries == 2 {
                        return Err(err.with_context(&format!(
                            "fail to {} {} after 3 reties",
                            request.method(),
                            request.url()
                        )));
                    }
                    retries += 1;
                    info!(
                        "will retry {} the {retries}th times on error {}",
                        request.url(),
                        err
                    );
                }
            }
            sleep(jitter(Duration::from_secs(10))).await;
        }
    }
}

impl Drop for APIClient {
    fn drop(&mut self) {
        if let Some(info) = mem::take(&mut self.login_info) {
            let cli = self.cli.clone();
            let req = self.build_log_out_request(info.lock().0.clone()).unwrap();
            tokio::task::spawn(async move {
                cli.execute(req).await.ok();
            });
        }
    }
}

fn json_from_slice<'a, T>(body: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    serde_json::from_slice::<T>(body).map_err(|e| {
        Error::Decode(format!(
            "fail to decode JSON response: {}, body: {}",
            e,
            String::from_utf8_lossy(body)
        ))
    })
}

impl Default for APIClient {
    fn default() -> Self {
        Self {
            cli: HttpClient::new(),
            scheme: "http".to_string(),
            endpoint: Url::parse("http://localhost:8080").unwrap(),
            host: "localhost".to_string(),
            port: 8000,
            tenant: None,
            warehouse: Arc::new(parking_lot::Mutex::new(None)),
            auth: Arc::new(BasicAuth::new("root", "")) as Arc<dyn Auth>,
            session_state: Arc::new(Mutex::new(SessionState::default())),
            wait_time_secs: None,
            max_rows_in_buffer: None,
            max_rows_per_page: None,
            connect_timeout: Duration::from_secs(10),
            page_request_timeout: Duration::from_secs(30),
            tls_ca_file: None,
            presign: PresignMode::Auto,
            route_hint: Arc::new(RouteHintGenerator::new()),
            disable_session_token: false,
            login_info: None,
        }
    }
}

struct RouteHintGenerator {
    nonce: AtomicU64,
    current: std::sync::Mutex<String>,
}

impl RouteHintGenerator {
    fn new() -> Self {
        let gen = Self {
            nonce: AtomicU64::new(0),
            current: std::sync::Mutex::new("".to_string()),
        };
        gen.next();
        gen
    }

    fn current(&self) -> String {
        let guard = self.current.lock().unwrap();
        guard.clone()
    }

    fn set(&self, hint: &str) {
        let mut guard = self.current.lock().unwrap();
        *guard = hint.to_string();
    }

    fn next(&self) -> String {
        let nonce = self.nonce.fetch_add(1, Ordering::AcqRel);
        let uuid = uuid::Uuid::new_v4();
        let current = format!("rh:{}:{:06}", uuid, nonce);
        let mut guard = self.current.lock().unwrap();
        *guard = current.clone();
        current
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn parse_dsn() -> Result<()> {
        let dsn = "databend://username:password@app.databend.com/test?wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&warehouse=wh&sslmode=disable";
        let client = APIClient::from_dsn(dsn).await?;
        assert_eq!(client.host, "app.databend.com");
        assert_eq!(client.endpoint, Url::parse("http://app.databend.com:80")?);
        assert_eq!(client.wait_time_secs, Some(10));
        assert_eq!(client.max_rows_in_buffer, Some(5000000));
        assert_eq!(client.max_rows_per_page, Some(10000));
        assert_eq!(client.tenant, None);
        assert_eq!(
            *client.warehouse.try_lock().unwrap(),
            Some("wh".to_string())
        );
        Ok(())
    }

    // #[tokio::test]
    // async fn parse_encoded_password() -> Result<()> {
    //     let dsn = "databend://username:3a%40SC(nYE1k%3D%7B%7BR@localhost";
    //     let client = APIClient::from_dsn(dsn).await?;
    //     assert_eq!(client.password, Some("3a@SC(nYE1k={{R".to_string()));
    //     Ok(())
    // }

    // #[tokio::test]
    // async fn parse_special_chars_password() -> Result<()> {
    //     let dsn = "databend://username:3a@SC(nYE1k={{R@localhost:8000";
    //     let client = APIClient::from_dsn(dsn).await?;
    //     assert_eq!(client.password, Some("3a@SC(nYE1k={{R".to_string()));
    //     Ok(())
    // }
}
