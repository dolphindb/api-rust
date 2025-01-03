//! Decimal type interface compatible to [`rust_decimal`]

use rust_decimal::Decimal;

use super::{
    super::{Decimal128, Decimal32, Decimal64},
    Scalar,
};

/// Helper interface for `Vector`.
pub trait DecimalInterface: Scalar {
    type LiteralValue;

    fn from_raw(num: Self::LiteralValue, scale: u32) -> Option<Self>;

    fn scale(&self) -> Option<u32>;

    fn mantissa(&self) -> Option<Self::LiteralValue>;

    fn rescale(&mut self, scale: u32);
}

impl Decimal32 {
    /// Returns a `Decimal` with a 32 bit `m` representation and corresponding `e` scale.
    ///
    /// # Errors
    ///
    /// Returns `None` if `scale` is > 9.
    pub fn from_raw(num: i32, scale: u32) -> Option<Self> {
        if scale > 9 {
            None
        } else {
            Some(Self(Some(Decimal::new(num as i64, scale))))
        }
    }

    pub fn scale(&self) -> Option<u32> {
        self.as_ref().map(|d| d.scale())
    }

    pub fn mantissa(&self) -> Option<i32> {
        if let Some(d) = self.as_ref() {
            let mantissa = d.mantissa();
            Some(mantissa as i32)
        } else {
            None
        }
    }

    pub fn rescale(&mut self, scale: u32) {
        if let Some(d) = &mut self.0 {
            d.rescale(scale)
        }
    }
}

impl Decimal64 {
    /// Returns a `Decimal` with a 64 bit `m` representation and corresponding `e` scale.
    ///
    /// # Errors
    ///
    /// Returns `None` if `scale` is > 18.
    pub fn from_raw(num: i64, scale: u32) -> Option<Self> {
        if scale > 18 {
            None
        } else {
            Some(Self(Some(Decimal::new(num, scale))))
        }
    }

    pub fn scale(&self) -> Option<u32> {
        self.as_ref().map(|d| d.scale())
    }

    pub fn mantissa(&self) -> Option<i64> {
        if let Some(d) = self.as_ref() {
            let mantissa = d.mantissa();
            Some(mantissa as i64)
        } else {
            None
        }
    }

    pub fn rescale(&mut self, scale: u32) {
        if let Some(d) = &mut self.0 {
            d.rescale(scale)
        }
    }
}

impl Decimal128 {
    /// Creates a `Decimal` using a 128 bit signed `m` representation and corresponding `e` scale.
    ///
    /// # Errors
    ///
    /// Returns `None` if `scale` is > 28 or if `num` exceeds the maximum supported 96 bits..
    pub fn from_raw(num: i128, scale: u32) -> Option<Self> {
        Decimal::try_from_i128_with_scale(num, scale)
            .ok()
            .map(|d| Self(Some(d)))
    }

    pub fn scale(&self) -> Option<u32> {
        self.as_ref().map(|d| d.scale())
    }

    pub fn mantissa(&self) -> Option<i128> {
        self.as_ref().map(|d| d.mantissa())
    }

    pub fn rescale(&mut self, scale: u32) {
        if let Some(d) = &mut self.0 {
            d.rescale(scale)
        }
    }
}

impl DecimalInterface for Decimal32 {
    type LiteralValue = i32;

    fn from_raw(num: Self::LiteralValue, scale: u32) -> Option<Self> {
        Self::from_raw(num, scale)
    }

    fn scale(&self) -> Option<u32> {
        self.scale()
    }

    fn mantissa(&self) -> Option<Self::LiteralValue> {
        self.mantissa()
    }

    fn rescale(&mut self, scale: u32) {
        self.rescale(scale)
    }
}

impl DecimalInterface for Decimal64 {
    type LiteralValue = i64;

    fn from_raw(num: Self::LiteralValue, scale: u32) -> Option<Self> {
        Self::from_raw(num, scale)
    }

    fn scale(&self) -> Option<u32> {
        self.scale()
    }

    fn mantissa(&self) -> Option<Self::LiteralValue> {
        self.mantissa()
    }

    fn rescale(&mut self, scale: u32) {
        self.rescale(scale)
    }
}

impl DecimalInterface for Decimal128 {
    type LiteralValue = i128;

    fn from_raw(num: Self::LiteralValue, scale: u32) -> Option<Self> {
        Self::from_raw(num, scale)
    }

    fn scale(&self) -> Option<u32> {
        self.scale()
    }

    fn mantissa(&self) -> Option<Self::LiteralValue> {
        self.mantissa()
    }

    fn rescale(&mut self, scale: u32) {
        self.rescale(scale)
    }
}
