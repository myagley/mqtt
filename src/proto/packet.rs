use super::BufMutExt;

/// An MQTT packet
#[derive(Clone, Debug)]
pub enum Packet {
	/// Ref: 3.2 CONNACK – Acknowledge connection request
	ConnAck {
		session_present: bool,
		return_code: super::ConnectReturnCode,
	},

	/// Ref: 3.1 CONNECT – Client requests a connection to a Server
	Connect {
		username: Option<String>,
		password: Option<String>,
		// TODO: will
		client_id: super::ClientId,
		keep_alive: std::time::Duration,
	},

	/// Ref: 3.12 PINGREQ – PING request
	PingReq,

	/// Ref: 3.13 PINGRESP – PING response
	PingResp,

	/// Ref: 3.4 PUBACK – Publish acknowledgement
	PubAck {
		packet_identifier: super::PacketIdentifier,
	},

	/// Ref: 3.7 PUBCOMP – Publish complete (QoS 2 publish received, part 3)
	PubComp {
		packet_identifier: super::PacketIdentifier,
	},

	/// 3.3 PUBLISH – Publish message
	Publish {
		packet_identifier_dup_qos: PacketIdentifierDupQoS,
		retain: bool,
		topic_name: String,
		payload: Vec<u8>,
	},

	/// Ref: 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
	PubRec {
		packet_identifier: super::PacketIdentifier,
	},

	/// Ref: 3.6 PUBREL – Publish release (QoS 2 publish received, part 2)
	PubRel {
		packet_identifier: super::PacketIdentifier,
	},

	/// Ref: 3.9 SUBACK – Subscribe acknowledgement
	SubAck {
		packet_identifier: super::PacketIdentifier,
		qos: Vec<SubAckQos>,
	},

	/// Ref: 3.8 SUBSCRIBE - Subscribe to topics
	Subscribe {
		packet_identifier: super::PacketIdentifier,
		subscribe_to: Vec<SubscribeTo>,
	},

	/// Ref: 3.11 UNSUBACK – Unsubscribe acknowledgement
	UnsubAck {
		packet_identifier: super::PacketIdentifier,
	},

	/// Ref: 3.10 UNSUBSCRIBE – Unsubscribe from topics
	Unsubscribe {
		packet_identifier: super::PacketIdentifier,
		unsubscribe_from: Vec<String>,
	},
}

impl Packet {
	/// The type of a [`Packet::ConnAck`]
	pub const CONNACK: u8 = 0x20;

	/// The type of a [`Packet::Connect`]
	pub const CONNECT: u8 = 0x10;

	/// The type of a [`Packet::PingReq`]
	pub const PINGREQ: u8 = 0xC0;

	/// The type of a [`Packet::PingResp`]
	pub const PINGRESP: u8 = 0xD0;

	/// The type of a [`Packet::PubAck`]
	pub const PUBACK: u8 = 0x40;

	/// The type of a [`Packet::PubComp`]
	pub const PUBCOMP: u8 = 0x70;

	/// The type of a [`Packet::Publish`]
	pub const PUBLISH: u8 = 0x30;

	/// The type of a [`Packet::PubRec`]
	pub const PUBREC: u8 = 0x50;

	/// The type of a [`Packet::PubRel`]
	pub const PUBREL: u8 = 0x60;

	/// The type of a [`Packet::SubAck`]
	pub const SUBACK: u8 = 0x90;

	/// The type of a [`Packet::Subscribe`]
	pub const SUBSCRIBE: u8 = 0x80;

	/// The type of a [`Packet::UnsubAck`]
	pub const UNSUBACK: u8 = 0xB0;

	/// The type of a [`Packet::Unsubscribe`]
	pub const UNSUBSCRIBE: u8 = 0xA0;
}

#[allow(clippy::doc_markdown)]
/// A combination of the packet identifier, dup flag and QoS that only allows valid combinations of these three properties.
/// Used in [`Packet::Publish`]
#[derive(Clone, Copy, Debug)]
pub enum PacketIdentifierDupQoS {
	AtMostOnce,
	AtLeastOnce(super::PacketIdentifier, bool),
	ExactlyOnce(super::PacketIdentifier, bool),
}

/// A subscription request.
#[derive(Clone, Debug)]
pub struct SubscribeTo {
	pub topic_filter: String,
	pub qos: QoS,
}

/// The level of reliability for a publication
///
/// Ref: 4.3 Quality of Service levels and protocol flows
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QoS {
	AtMostOnce,
	AtLeastOnce,
	ExactlyOnce,
}

impl From<QoS> for u8 {
	fn from(qos: QoS) -> Self {
		match qos {
			QoS::AtMostOnce => 0,
			QoS::AtLeastOnce => 1,
			QoS::ExactlyOnce => 2,
		}
	}
}

#[allow(clippy::doc_markdown)]
/// QoS returned in a SUBACK packet. Either one of the [`QoS`] values, or an error code.
#[derive(Clone, Copy, Debug)]
pub enum SubAckQos {
	Success(QoS),
	Failure,
}

/// A tokio codec that encodes and decodes MQTT packets.
///
/// Ref: 2 MQTT Control Packet format
#[derive(Debug, Default)]
pub struct PacketCodec {
	decoder_state: PacketDecoderState,
}

#[derive(Debug)]
pub enum PacketDecoderState {
	Empty,
	HaveFirstByte { first_byte: u8, remaining_length: super::RemainingLengthCodec },
	HaveFixedHeader { first_byte: u8, remaining_length: usize },
}

impl Default for PacketDecoderState {
	fn default() -> Self {
		PacketDecoderState::Empty
	}
}

impl tokio::codec::Decoder for PacketCodec {
	type Item = Packet;
	type Error = super::DecodeError;

	fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
		let (first_byte, mut src) = loop {
			match &mut self.decoder_state {
				PacketDecoderState::Empty => {
					let first_byte = match src.try_get_u8() {
						Ok(first_byte) => first_byte,
						Err(_) => return Ok(None),
					};
					self.decoder_state = PacketDecoderState::HaveFirstByte { first_byte, remaining_length: Default::default() };
				},

				PacketDecoderState::HaveFirstByte { first_byte, remaining_length } => match remaining_length.decode(src)? {
					Some(remaining_length) => self.decoder_state = PacketDecoderState::HaveFixedHeader { first_byte: *first_byte, remaining_length },
					None => return Ok(None),
				},

				PacketDecoderState::HaveFixedHeader { first_byte, remaining_length } => {
					if src.len() < *remaining_length {
						return Ok(None);
					}

					let first_byte = *first_byte;
					let src = src.split_to(*remaining_length);
					self.decoder_state = PacketDecoderState::Empty;
					break (first_byte, src);
				},
			}
		};

		match (first_byte & 0xF0, first_byte & 0x0F, src.len()) {
			(Packet::CONNACK, 0, 2) => {
				let flags = src.try_get_u8()?;
				let session_present = match flags {
					0x00 => false,
					0x01 => true,
					flags => return Err(super::DecodeError::UnrecognizedConnAckFlags(flags)),
				};

				let return_code: super::ConnectReturnCode = src.try_get_u8()?.into();

				Ok(Some(Packet::ConnAck {
					session_present,
					return_code,
				}))
			},

			(Packet::PINGRESP, 0, 0) =>
				Ok(Some(Packet::PingResp)),

			(Packet::PUBACK, 0, 2) => {
				let packet_identifier = src.try_get_u16_be()?;
				let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
					Some(packet_identifier) => packet_identifier,
					None => return Err(super::DecodeError::ZeroPacketIdentifier),
				};

				Ok(Some(Packet::PubAck {
					packet_identifier,
				}))
			},

			(Packet::PUBCOMP, 0, 2) => {
				let packet_identifier = src.try_get_u16_be()?;
				let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
					Some(packet_identifier) => packet_identifier,
					None => return Err(super::DecodeError::ZeroPacketIdentifier),
				};

				Ok(Some(Packet::PubComp {
					packet_identifier,
				}))
			},

			(Packet::PUBLISH, flags, _) => {
				let dup = (flags & 0x08) != 0;
				let retain = (flags & 0x01) != 0;

				let topic_name = super::Utf8StringCodec::default().decode(&mut src)?.ok_or(super::DecodeError::IncompletePacket)?;

				let packet_identifier_dup_qos = match (flags & 0x06) >> 1 {
					0x00 => PacketIdentifierDupQoS::AtMostOnce,

					0x01 => {
						let packet_identifier = src.try_get_u16_be()?;
						let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
							Some(packet_identifier) => packet_identifier,
							None => return Err(super::DecodeError::ZeroPacketIdentifier),
						};
						PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, dup)
					},

					0x02 => {
						let packet_identifier = src.try_get_u16_be()?;
						let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
							Some(packet_identifier) => packet_identifier,
							None => return Err(super::DecodeError::ZeroPacketIdentifier),
						};
						PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, dup)
					},

					qos => return Err(super::DecodeError::UnrecognizedQoS(qos)),
				};

				let mut payload = vec![0_u8; src.len()];
				payload.copy_from_slice(&src.take());

				Ok(Some(Packet::Publish {
					packet_identifier_dup_qos,
					retain,
					topic_name,
					payload,
				}))
			},

			(Packet::PUBREC, 0, 2) => {
				let packet_identifier = src.try_get_u16_be()?;
				let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
					Some(packet_identifier) => packet_identifier,
					None => return Err(super::DecodeError::ZeroPacketIdentifier),
				};

				Ok(Some(Packet::PubRec {
					packet_identifier,
				}))
			},

			(Packet::PUBREL, 2, 2) => {
				let packet_identifier = src.try_get_u16_be()?;
				let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
					Some(packet_identifier) => packet_identifier,
					None => return Err(super::DecodeError::ZeroPacketIdentifier),
				};

				Ok(Some(Packet::PubRel {
					packet_identifier,
				}))
			},

			(Packet::SUBACK, 0, remaining_length) => {
				let packet_identifier = src.try_get_u16_be()?;
				let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
					Some(packet_identifier) => packet_identifier,
					None => return Err(super::DecodeError::ZeroPacketIdentifier),
				};

				let mut qos = vec![];
				for _ in 2..remaining_length {
					qos.push(match src.try_get_u8()? {
						0x00 => SubAckQos::Success(QoS::AtMostOnce),
						0x01 => SubAckQos::Success(QoS::AtLeastOnce),
						0x02 => SubAckQos::Success(QoS::ExactlyOnce),
						0x80 => SubAckQos::Failure,
						qos => return Err(super::DecodeError::UnrecognizedQoS(qos)),
					});
				}

				Ok(Some(Packet::SubAck {
					packet_identifier,
					qos,
				}))
			},

			(Packet::UNSUBACK, 0, 2) => {
				let packet_identifier = src.try_get_u16_be()?;
				let packet_identifier = match super::PacketIdentifier::new(packet_identifier) {
					Some(packet_identifier) => packet_identifier,
					None => return Err(super::DecodeError::ZeroPacketIdentifier),
				};

				Ok(Some(Packet::UnsubAck {
					packet_identifier,
				}))
			},

			(packet_type, flags, remaining_length) =>
				Err(super::DecodeError::UnrecognizedPacket { packet_type, flags, remaining_length }),
		}
	}
}

impl tokio::codec::Encoder for PacketCodec {
	type Item = Packet;
	type Error = super::EncodeError;

	fn encode(&mut self, item: Self::Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
		dst.reserve(std::mem::size_of::<u8>() + 4 * std::mem::size_of::<u8>());

		match item {
			Packet::ConnAck { .. } => unimplemented!(),

			Packet::Connect { username, password, client_id, keep_alive } => {
				dst.append_u8(Packet::CONNECT);

				let mut remaining_dst = bytes::BytesMut::new();

				remaining_dst.extend_from_slice(b"\x00\x04MQTT");
				remaining_dst.append_u8(0x04_u8);
				{
					let mut connect_flags = 0x00_u8;
					if username.is_some() {
						connect_flags |= 0x80;
					}
					if password.is_some() {
						connect_flags |= 0x40;
					}
					match client_id {
						super::ClientId::ServerGenerated |
						super::ClientId::IdWithCleanSession(_) => {
							connect_flags |= 0x02;
						},
						super::ClientId::IdWithExistingSession(_) => (),
					}
					remaining_dst.append_u8(connect_flags);
				}
				{
					#[allow(clippy::cast_possible_truncation)]
					let keep_alive = match keep_alive {
						keep_alive if keep_alive.as_secs() <= u64::from(u16::max_value()) => keep_alive.as_secs() as u16,
						keep_alive => return Err(super::EncodeError::KeepAliveTooHigh(keep_alive)),
					};
					remaining_dst.append_u16_be(keep_alive);
				}
				match client_id {
					super::ClientId::ServerGenerated => super::Utf8StringCodec::default().encode("".to_string(), &mut remaining_dst)?,
					super::ClientId::IdWithCleanSession(id) |
					super::ClientId::IdWithExistingSession(id) => super::Utf8StringCodec::default().encode(id, &mut remaining_dst)?,
				}
				if let Some(username) = username {
					super::Utf8StringCodec::default().encode(username, &mut remaining_dst)?
				}
				if let Some(password) = password {
					super::Utf8StringCodec::default().encode(password, &mut remaining_dst)?
				}

				super::RemainingLengthCodec::default().encode(remaining_dst.len(), dst)?;
				dst.extend_from_slice(&remaining_dst);
			},

			Packet::PubAck { packet_identifier } => {
				dst.append_u8(Packet::PUBACK);
				super::RemainingLengthCodec::default().encode(std::mem::size_of::<u16>(), dst)?;
				dst.append_packet_identifier(packet_identifier);
			},

			Packet::PubComp { packet_identifier } => {
				dst.append_u8(Packet::PUBCOMP);
				super::RemainingLengthCodec::default().encode(std::mem::size_of::<u16>(), dst)?;
				dst.append_packet_identifier(packet_identifier);
			},

			Packet::Publish { packet_identifier_dup_qos, retain, topic_name, payload } => {
				let mut packet_type = Packet::PUBLISH;
				packet_type |= match packet_identifier_dup_qos {
					PacketIdentifierDupQoS::AtMostOnce => 0x00,
					PacketIdentifierDupQoS::AtLeastOnce(_, true) => 0x0A,
					PacketIdentifierDupQoS::AtLeastOnce(_, false) => 0x02,
					PacketIdentifierDupQoS::ExactlyOnce(_, true) => 0x0C,
					PacketIdentifierDupQoS::ExactlyOnce(_, false) => 0x04,
				};
				if retain {
					packet_type |= 0x01;
				}
				dst.append_u8(packet_type);

				let mut remaining_dst = bytes::BytesMut::new();

				super::Utf8StringCodec::default().encode(topic_name, &mut remaining_dst)?;

				match packet_identifier_dup_qos {
					PacketIdentifierDupQoS::AtMostOnce => (),
					PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, _) |
					PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, _) =>
						remaining_dst.append_packet_identifier(packet_identifier),
				}

				remaining_dst.extend_from_slice(&payload);

				super::RemainingLengthCodec::default().encode(remaining_dst.len(), dst)?;
				dst.extend_from_slice(&remaining_dst);
			},

			Packet::PubRec { packet_identifier } => {
				dst.append_u8(Packet::PUBREC);
				super::RemainingLengthCodec::default().encode(std::mem::size_of::<u16>(), dst)?;
				dst.append_packet_identifier(packet_identifier);
			},

			Packet::PubRel { packet_identifier } => {
				dst.append_u8(Packet::PUBREL | 0x02);
				super::RemainingLengthCodec::default().encode(std::mem::size_of::<u16>(), dst)?;
				dst.append_packet_identifier(packet_identifier);
			},

			Packet::PingReq => {
				dst.append_u8(Packet::PINGREQ);
				dst.append_u8(0x00);
			},

			Packet::PingResp => unimplemented!(),

			Packet::SubAck { .. } => unimplemented!(),

			Packet::Subscribe { packet_identifier, subscribe_to } => {
				dst.append_u8(Packet::SUBSCRIBE | 0x02);

				let mut remaining_dst = bytes::BytesMut::new();

				remaining_dst.append_packet_identifier(packet_identifier);

				for SubscribeTo { topic_filter, qos } in subscribe_to {
					super::Utf8StringCodec::default().encode(topic_filter, &mut remaining_dst)?;
					remaining_dst.append_u8(qos.into());
				}

				super::RemainingLengthCodec::default().encode(remaining_dst.len(), dst)?;
				dst.extend_from_slice(&remaining_dst);
			},

			Packet::UnsubAck { .. } => unimplemented!(),

			Packet::Unsubscribe { packet_identifier, unsubscribe_from } => {
				dst.append_u8(Packet::UNSUBSCRIBE | 0x02);

				let mut remaining_dst = bytes::BytesMut::new();

				remaining_dst.append_packet_identifier(packet_identifier);

				for unsubscribe_from in unsubscribe_from {
					super::Utf8StringCodec::default().encode(unsubscribe_from, &mut remaining_dst)?;
				}

				super::RemainingLengthCodec::default().encode(remaining_dst.len(), dst)?;
				dst.extend_from_slice(&remaining_dst);
			},
		}

		Ok(())
	}
}
