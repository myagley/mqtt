/*!
 * MQTT protocol types.
 */

use bytes::{ Buf, BufMut };

mod packet;

pub use self::packet::{ Packet, PacketCodec, PacketIdentifier, PacketIdentifierDupQoS, QoS, SubAckQos, SubscribeTo };

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
	fn decode<T>(src: &mut T) -> Result<String, DecodeError> where T: bytes::Buf {
		let len = src.try_get_u16_be()? as usize;

		if src.remaining() < len {
			return Err(DecodeError::IncompletePacket);
		}

		let mut s = vec![0_u8; len];
		src.copy_to_slice(&mut s);
		let s = match String::from_utf8(s) {
			Ok(s) => s,
			Err(err) => return Err(DecodeError::StringNotUtf8(err)),
		};
		Ok(s)
	}
}

impl tokio::codec::Encoder for Utf8StringCodec {
	type Item = String;
	type Error = EncodeError;

	fn encode(&mut self, item: Self::Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
		dst.reserve(std::mem::size_of::<u16>() + item.len());

		#[allow(clippy::cast_possible_truncation)]
		match item.len() {
			len if len <= u16::max_value() as usize => dst.put_u16_be(len as u16),
			len => return Err(EncodeError::StringTooLarge(len)),
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
	fn decode<T>(src: &mut T) -> Result<Option<usize>, DecodeError> where T: bytes::Buf {
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
				return Err(DecodeError::RemainingLengthTooHigh);
			}
		}

		Ok(Some(result))
	}
}

impl tokio::codec::Encoder for RemainingLengthCodec {
	type Item = usize;
	type Error = EncodeError;

	fn encode(&mut self, mut item: Self::Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
		dst.reserve(4 * std::mem::size_of::<u8>());

		let original = item;

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
				return Err(EncodeError::RemainingLengthTooHigh(original));
			}
		}

		Ok(())
	}
}

#[derive(Debug)]
pub enum DecodeError {
	IncompletePacket,
	Io(std::io::Error),
	RemainingLengthTooHigh,
	StringNotUtf8(std::string::FromUtf8Error),
	UnrecognizedConnAckFlags(u8),
	UnrecognizedPacket { packet_type: u8, flags: u8, remaining_length: usize },
	UnrecognizedQoS(u8),
	ZeroPacketIdentifier,
}

impl std::fmt::Display for DecodeError {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			DecodeError::IncompletePacket => write!(f, "packet is truncated"),
			DecodeError::Io(err) => write!(f, "I/O error: {}", err),
			DecodeError::RemainingLengthTooHigh => write!(f, "remaining length is too high to be decoded"),
			DecodeError::StringNotUtf8(err) => err.fmt(f),
			DecodeError::UnrecognizedConnAckFlags(flags) => write!(f, "could not parse CONNACK flags 0x{:02X}", flags),
			DecodeError::UnrecognizedPacket { packet_type, flags, remaining_length } =>
				write!(
					f,
					"could not identify packet with type 0x{:1X}, flags 0x{:1X} and remaining length {}",
					packet_type,
					flags,
					remaining_length,
				),
			DecodeError::UnrecognizedQoS(qos) => write!(f, "could not parse QoS 0x{:02X}", qos),
			DecodeError::ZeroPacketIdentifier => write!(f, "packet identifier is 0"),
		}
	}
}

impl std::error::Error for DecodeError {
	fn source(&self) -> Option<&(std::error::Error + 'static)> {
		#[allow(clippy::match_same_arms)]
		match self {
			DecodeError::IncompletePacket => None,
			DecodeError::Io(err) => Some(err),
			DecodeError::RemainingLengthTooHigh => None,
			DecodeError::StringNotUtf8(err) => Some(err),
			DecodeError::UnrecognizedConnAckFlags(_) => None,
			DecodeError::UnrecognizedPacket { .. } => None,
			DecodeError::UnrecognizedQoS(_) => None,
			DecodeError::ZeroPacketIdentifier => None,
		}
	}
}

impl From<std::io::Error> for DecodeError {
	fn from(err: std::io::Error) -> Self {
		DecodeError::Io(err)
	}
}

#[derive(Debug)]
pub enum EncodeError {
	Io(std::io::Error),
	KeepAliveTooHigh(std::time::Duration),
	RemainingLengthTooHigh(usize),
	StringTooLarge(usize),
}

impl std::fmt::Display for EncodeError {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			EncodeError::Io(err) => write!(f, "I/O error: {}", err),
			EncodeError::KeepAliveTooHigh(keep_alive) => write!(f, "keep-alive {:?} is too high", keep_alive),
			EncodeError::RemainingLengthTooHigh(len) => write!(f, "remaining length {} is too high to be encoded", len),
			EncodeError::StringTooLarge(len) => write!(f, "string of length {} is too large to be encoded", len),
		}
	}
}

impl std::error::Error for EncodeError {
	fn source(&self) -> Option<&(std::error::Error + 'static)> {
		#[allow(clippy::match_same_arms)]
		match self {
			EncodeError::Io(err) => Some(err),
			EncodeError::KeepAliveTooHigh(_) => None,
			EncodeError::RemainingLengthTooHigh(_) => None,
			EncodeError::StringTooLarge(_) => None,
		}
	}
}

impl From<std::io::Error> for EncodeError {
	fn from(err: std::io::Error) -> Self {
		EncodeError::Io(err)
	}
}

trait BufExt {
	fn try_get_u8(&mut self) -> Result<u8, DecodeError>;
	fn try_get_u16_be(&mut self) -> Result<u16, DecodeError>;
}

impl<T> BufExt for T where T: bytes::Buf {
	fn try_get_u8(&mut self) -> Result<u8, DecodeError> {
		if self.remaining() < std::mem::size_of::<u8>() {
			return Err(DecodeError::IncompletePacket);
		}

		Ok(self.get_u8())
	}

	fn try_get_u16_be(&mut self) -> Result<u16, DecodeError> {
		if self.remaining() < std::mem::size_of::<u16>() {
			return Err(DecodeError::IncompletePacket);
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
