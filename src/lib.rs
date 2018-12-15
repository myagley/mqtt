/*!
 * This crate contains an implementation of an MQTT client.
 */

#![deny(unused_extern_crates, warnings)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(
	clippy::default_trait_access,
	clippy::pub_enum_variant_names,
	clippy::single_match_else,
	clippy::stutter,
	clippy::too_many_arguments,
	clippy::use_self,
)]

mod client;
pub use self::client::{ Client, Error, IoSource, Publication, PublishHandle, ReceivedPublication, UpdateSubscriptionHandle };

mod logging_framed;

pub mod proto;
