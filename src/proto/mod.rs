/*!
 * MQTT protocol types.
 */

use bytes::{ Buf, BufMut };

mod packet;

pub use self::packet::{ Packet, PacketCodec, PacketIdentifier, PacketIdentifierDupQoS, QoS, SubscribeTo };

/// The client ID
///
/// Refs:
/// - 3.1.3.1 Client Identifier
/// - 3.1.2.4 Clean Session
#[derive(Clone, Debug)]
pub enum ClientId {
	ServerGenerated,
	IdWithCleanSession(String),
	IdWithExistingSession(String),
}

/// The return code for a connection attempt
///
/// Ref: 3.2.2.3 Connect Return code
#[derive(Clone, Copy, Debug)]
pub enum ConnectReturnCode {
	Accepted,
	Refused(ConnectionRefusedReason),
}

/// The reason the connection was refused by the server
///
/// Ref: 3.2.2.3 Connect Return code
#[derive(Clone, Copy, Debug)]
pub enum ConnectionRefusedReason {
	UnacceptableProtocolVersion,
	IdentifierRejected,
	ServerUnavailable,
	BadUserNameOrPassword,
	NotAuthorized,
	Other(u8),
}

impl From<u8> for ConnectReturnCode {
	fn from(code: u8) -> Self {
		match code {
			0x00 => ConnectReturnCode::Accepted,
			0x01 => ConnectReturnCode::Refused(ConnectionRefusedReason::UnacceptableProtocolVersion),
			0x02 => ConnectReturnCode::Refused(ConnectionRefusedReason::IdentifierRejected),
			0x03 => ConnectReturnCode::Refused(ConnectionRefusedReason::ServerUnavailable),
			0x04 => ConnectReturnCode::Refused(ConnectionRefusedReason::BadUserNameOrPassword),
			0x05 => ConnectReturnCode::Refused(ConnectionRefusedReason::NotAuthorized),
			code => ConnectReturnCode::Refused(ConnectionRefusedReason::Other(code)),
		}
	}
}

impl From<ConnectReturnCode> for u8 {
	fn from(code: ConnectReturnCode) -> Self {
		match code {
			ConnectReturnCode::Accepted => 0x00,
			ConnectReturnCode::Refused(ConnectionRefusedReason::UnacceptableProtocolVersion) => 0x01,
			ConnectReturnCode::Refused(ConnectionRefusedReason::IdentifierRejected) => 0x02,
			ConnectReturnCode::Refused(ConnectionRefusedReason::ServerUnavailable) => 0x03,
			ConnectReturnCode::Refused(ConnectionRefusedReason::BadUserNameOrPassword) => 0x04,
			ConnectReturnCode::Refused(ConnectionRefusedReason::NotAuthorized) => 0x05,
			ConnectReturnCode::Refused(ConnectionRefusedReason::Other(code)) => code,
		}
	}
}

/// A tokio codec that encodes and decodes MQTT-format strings.
///
/// Strings are prefixed with a two-byte big-endian length and are encoded as utf-8.
///
/// Ref: 1.5.3 UTF-8 encoded strings
#[derive(Debug)]
pub struct Utf8StringCodec;

impl Utf8StringCodec {
	fn decode<T>(src: &mut T) -> std::io::Result<String> where T: bytes::Buf {
		let len = src.try_get_u16_be()? as usize;

		if src.remaining() < len {
			return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"));
		}

		let mut s = vec![0_u8; len];
		src.copy_to_slice(&mut s);
		let s = match String::from_utf8(s) {
			Ok(s) => s,
			Err(err) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, err)),
		};
		Ok(s)
	}
}

impl tokio::codec::Encoder for Utf8StringCodec {
	type Item = String;
	type Error = std::io::Error;

	fn encode(&mut self, item: Self::Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
		dst.reserve(std::mem::size_of::<u16>() + item.len());

		#[allow(clippy::cast_possible_truncation)]
		match item.len() {
			len if len <= u16::max_value() as usize => dst.put_u16_be(len as u16),
			_ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "string too large")),
		}

		dst.put_slice(item.as_bytes());

		Ok(())
	}
}

/// A tokio codec that encodes and decodes MQTT-format "remaining length" numbers.
///
/// These numbers are encoded with a variable-length scheme that uses the MSB of each byte as a continuation bit.
///
/// Ref: 2.2.3 Remaining Length
#[derive(Debug)]
pub struct RemainingLengthCodec;

impl RemainingLengthCodec {
	fn decode<T>(src: &mut T) -> std::io::Result<Option<usize>> where T: bytes::Buf {
		let mut result = 0;
		let mut num_bytes_read = 0;

		loop {
			let encoded_byte =
				if src.remaining() >= std::mem::size_of::<u8>() {
					src.get_u8()
				}
				else {
					return Ok(None);
				};

			result |= ((encoded_byte & 0x7F) as usize) << (num_bytes_read * 7);

			num_bytes_read += 1;

			if encoded_byte & 0x80 == 0 {
				break;
			}

			if num_bytes_read == 4 {
				return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "number too large"));
			}
		}

		Ok(Some(result))
	}
}

impl tokio::codec::Encoder for RemainingLengthCodec {
	type Item = usize;
	type Error = std::io::Error;

	fn encode(&mut self, mut item: Self::Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
		dst.reserve(4 * std::mem::size_of::<u8>());

		loop {
			#[allow(clippy::cast_possible_truncation)]
			let mut encoded_byte = (item & 0x7F) as u8;

			item >>= 7;

			if item > 0 {
				encoded_byte |= 0x80;
			}

			dst.put_u8(encoded_byte);

			if item == 0 {
				break;
			}

			if dst.len() == 4 {
				return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "number too large"));
			}
		}

		Ok(())
	}
}

trait BufExt {
	fn try_get_u8(&mut self) -> std::io::Result<u8>;
	fn try_get_u16_be(&mut self) -> std::io::Result<u16>;
}

impl<T> BufExt for T where T: bytes::Buf {
	fn try_get_u8(&mut self) -> std::io::Result<u8> {
		if self.remaining() < std::mem::size_of::<u8>() {
			return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"));
		}

		Ok(self.get_u8())
	}

	fn try_get_u16_be(&mut self) -> std::io::Result<u16> {
		if self.remaining() < std::mem::size_of::<u16>() {
			return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"));
		}

		Ok(self.get_u16_be())
	}
}

trait BufMutExt {
	fn append_u8(&mut self, n: u8);
	fn append_u16_be(&mut self, n: u16);
}

impl BufMutExt for bytes::BytesMut {
	fn append_u8(&mut self, n: u8) {
		self.reserve(std::mem::size_of::<u8>());
		self.put_u8(n);
	}

	fn append_u16_be(&mut self, n: u16) {
		self.reserve(std::mem::size_of::<u16>());
		self.put_u16_be(n);
	}
}
