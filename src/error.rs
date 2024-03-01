use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("The object can't be converted to bool scalar.")]
    GetBoolFail,
    #[error("The object can't be converted to char scalar.")]
    GetCharFail,
    #[error("The object can't be converted to short scalar.")]
    GetShortFail,
    #[error("The object can't be converted to int scalar.")]
    GetIntFail,
}
