//! DolphinDB subscriber client for receiving data from stream tables.
//!
//! # Organization
//! * [`request`] contains server and table info to subscribe.
//! * [`subscriber`] manages subscriptions.
//! * [`message`] contains the table data.

pub mod message;
mod message_parser;
mod reconnect;
pub mod request;
pub mod subscriber;
