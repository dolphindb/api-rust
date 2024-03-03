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
    #[error("The object can't be converted to long scalar.")]
    GetLongFail,
    #[error("The object can't be converted to float scalar.")]
    GetFloatFail,
    #[error("The object can't be converted to double scalar.")]
    GetDoubleFail,
    #[error("The object can't be converted to string scalar.")]
    GetStringFail,
}
