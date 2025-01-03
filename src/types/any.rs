use std::fmt::{self, Display};

use super::{Constant, ConstantImpl, DataType};

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct Any(pub(crate) ConstantImpl);

impl Any {
    pub const DATA_BYTE: DataType = DataType::Any;

    pub fn new(val: ConstantImpl) -> Self {
        Self(val)
    }

    pub const fn data_type() -> DataType {
        Self::DATA_BYTE
    }

    pub fn raw_data_type(&self) -> DataType {
        self.0.data_type()
    }

    pub fn set(&mut self, val: ConstantImpl) {
        self.0 = val
    }

    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    pub const fn get(&self) -> &ConstantImpl {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut ConstantImpl {
        &mut self.0
    }

    pub fn into_inner(self) -> ConstantImpl {
        self.0
    }
}

impl Display for Any {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<ConstantImpl> for Any {
    fn from(value: ConstantImpl) -> Self {
        Self::new(value)
    }
}
