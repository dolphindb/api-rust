use std::time::Duration;

use crate::types::VectorImpl;

#[derive(Debug, Clone)]
pub struct Request {
    pub(crate) table_name: String,
    pub(crate) action_name: String,
    pub(crate) msg_as_table: bool,
    pub(crate) offset: i64,
    pub(crate) reconnect: bool,
    pub(crate) reconnect_timeout: Duration,

    pub(crate) filter: Option<VectorImpl>,

    pub(crate) auth: Option<(String, String)>,
}

impl Request {
    pub fn new(table_name: String, action_name: String) -> Self {
        Request {
            table_name,
            action_name,
            msg_as_table: false,
            offset: -1,
            reconnect: false,
            reconnect_timeout: Duration::from_millis(100),

            filter: None,

            auth: None,
        }
    }

    pub fn with_msg_as_table(&mut self, msg_as_table: bool) -> &mut Self {
        self.msg_as_table = msg_as_table;
        self
    }

    pub fn with_offset(&mut self, offset: i64) -> &mut Self {
        self.offset = offset;
        self
    }

    pub fn with_reconnect(&mut self, reconnect: bool) -> &mut Self {
        self.reconnect = reconnect;
        self
    }

    pub fn with_reconnect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.reconnect_timeout = timeout;
        self
    }

    pub fn with_filter(&mut self, filter: VectorImpl) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_auth(&mut self, auth: (impl Into<String>, impl Into<String>)) -> &mut Self {
        self.auth = Some((auth.0.into(), auth.1.into()));
        self
    }
}
