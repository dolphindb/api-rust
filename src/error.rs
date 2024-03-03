use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("The object is not a bool scalar.")]
    NotBoolScalar,
    #[error("The object is not a to char scalar.")]
    NotCharScalar,
    #[error("The object is not a to short scalar.")]
    NotShortScalar,
    #[error("The object is not a to int scalar.")]
    NotIntScalar,
    #[error("The object is not a to long scalar.")]
    NotLongScalar,
    #[error("The object is not a to float scalar.")]
    NotFloatScalar,
    #[error("The object is not a to double scalar.")]
    NotDoubleScalar,
    #[error("The object is not a to string scalar.")]
    NotStringScalar,

    #[error("The object doesn't support this interface.")]
    NotSupportInterface,
}
