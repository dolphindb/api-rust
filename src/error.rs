use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("The object is not a bool scalar.")]
    NotBoolScalar,
    #[error("The object is not a char scalar.")]
    NotCharScalar,
    #[error("The object is not a short scalar.")]
    NotShortScalar,
    #[error("The object is not a int scalar.")]
    NotIntScalar,
    #[error("The object is not a long scalar.")]
    NotLongScalar,
    #[error("The object is not a float scalar.")]
    NotFloatScalar,
    #[error("The object is not a double scalar.")]
    NotDoubleScalar,
    #[error("The object is not a string scalar.")]
    NotStringScalar,
    #[error("The object is not a int nor 32-bit temporal scalar.")]
    NotIntNorTemporal32Scalar,
    #[error("The object is not a long nor 64-bit temporal scalar.")]
    NotLongNorTemporal64Scalar,

    #[error("The object is not a date scalar.")]
    NotDateScalar,
    #[error("The object is not a month scalar.")]
    NotMonthScalar,
    #[error("The object is not a time scalar.")]
    NotTimeScalar,
    #[error("The object is not a minute scalar.")]
    NotMinuteScalar,
    #[error("The object is not a second scalar.")]
    NotSecondScalar,
    #[error("The object is not a date time scalar.")]
    NotDateTimeScalar,
    #[error("The object is not a date hour scalar.")]
    NotDateHourScalar,
    #[error("The object is not a time stamp scalar.")]
    NotTimeStampScalar,
    #[error("The object is not a nano time scalar.")]
    NotNanoTimeScalar,
    #[error("The object is not a nano time stamp scalar.")]
    NotNanoTimeStampScalar,
    #[error("The object is not a uuid scalar.")]
    NotUuidScalar,

    #[error("The object is not a scalar.")]
    NotScalarKind,
    #[error("The object is not a vector.")]
    NotVectorKind,
    #[error("The object is not a pair.")]
    NotPair,
    #[error("The object is not a set.")]
    NotSet,
    #[error("The object is not a dictionary.")]
    NotDictionary,

    #[error("The objects from one pair should have the same scalar type.")]
    PairDataTypeMismatch,
    #[error("The objects from one set should have the same scalar type.")]
    SetDataTypeMismatch,

    #[error("The object can't convert to target type.")]
    ConvertFail,
    #[error("Invalid data.")]
    InvalidData,
}
