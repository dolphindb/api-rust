use crate::types::VectorImpl;

#[derive(Debug, Clone)]
pub struct Message {
    offset: i64,
    topic: String,

    msg: VectorImpl,
}

impl Message {
    pub(crate) fn new(offset: i64, topic: String, msg: VectorImpl) -> Self {
        Self { offset, topic, msg }
    }

    pub fn topic(&self) -> &String {
        &self.topic
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn msg(&self) -> &VectorImpl {
        &self.msg
    }
}
