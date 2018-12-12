#[derive(Debug)]
pub(super) struct State {
	packet_identifiers: super::PacketIdentifiers,

	at_least_once: std::collections::HashMap<crate::proto::PacketIdentifier, futures::sync::oneshot::Sender<()>>,
}

impl State {
	pub(super) fn poll(
		&mut self,
		packet: &mut Option<crate::proto::Packet>,
		publish_requests_waiting_to_be_sent: Vec<super::PublishRequest>,
	) -> std::io::Result<(Vec<crate::proto::Packet>, Vec<crate::Publication>)> {
		let mut packets_waiting_to_be_sent = vec![];
		let mut publications_received = vec![];

		match packet.take() {
			Some(crate::proto::Packet::PubAck { packet_identifier }) => match self.at_least_once.remove(&packet_identifier) {
				Some(ack_sender) => {
					self.packet_identifiers.discard(packet_identifier);

					match ack_sender.send(()) {
						Ok(()) => (),
						Err(_) => log::debug!("could not send ack for publish request because ack receiver has been dropped"),
					}
				},
				None => log::warn!("ignoring PUBACK for a PUBLISH we never sent"),
			},

			Some(crate::proto::Packet::PubComp { .. }) => unimplemented!(),

			Some(crate::proto::Packet::Publish { packet_identifier_dup_qos, retain, topic_name, payload }) => {
				let qos = match packet_identifier_dup_qos {
					crate::proto::PacketIdentifierDupQoS::AtMostOnce => crate::proto::QoS::AtMostOnce,
					crate::proto::PacketIdentifierDupQoS::AtLeastOnce(_, _) => crate::proto::QoS::AtLeastOnce,
					crate::proto::PacketIdentifierDupQoS::ExactlyOnce(_, _) => crate::proto::QoS::ExactlyOnce,
				};

				publications_received.push(crate::Publication {
					topic_name,
					qos,
					retain,
					payload,
				});

				match packet_identifier_dup_qos {
					crate::proto::PacketIdentifierDupQoS::AtMostOnce => (),
					crate::proto::PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, _) =>
						packets_waiting_to_be_sent.push(crate::proto::Packet::PubAck {
							packet_identifier,
						}),
					crate::proto::PacketIdentifierDupQoS::ExactlyOnce(_, _) => unimplemented!(),
				}
			},

			Some(crate::proto::Packet::PubRec { .. }) => unimplemented!(),

			Some(crate::proto::Packet::PubRel { .. }) => unimplemented!(),

			other => *packet = other,
		}

		for super::PublishRequest { publication, ack_sender } in publish_requests_waiting_to_be_sent {
			match publication.qos {
				crate::proto::QoS::AtMostOnce => {
					packets_waiting_to_be_sent.push(crate::proto::Packet::Publish {
						packet_identifier_dup_qos: crate::proto::PacketIdentifierDupQoS::AtMostOnce,
						retain: publication.retain,
						topic_name: publication.topic_name,
						payload: publication.payload,
					});

					match ack_sender.send(()) {
						Ok(()) => (),
						Err(_) => log::debug!("could not send ack for publish request because ack receiver has been dropped"),
					}
				},

				crate::proto::QoS::AtLeastOnce => {
					let packet_identifier = self.packet_identifiers.reserve();

					packets_waiting_to_be_sent.push(crate::proto::Packet::Publish {
						packet_identifier_dup_qos: crate::proto::PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
						retain: publication.retain,
						topic_name: publication.topic_name,
						payload: publication.payload,
					});

					self.at_least_once.insert(packet_identifier, ack_sender);
				},

				crate::proto::QoS::ExactlyOnce => unimplemented!(),
			}
		}

		Ok((packets_waiting_to_be_sent, publications_received))
	}
}

impl Default for State {
	fn default() -> Self {
		State {
			packet_identifiers: Default::default(),

			at_least_once: Default::default(),
		}
	}
}
