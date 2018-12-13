#[derive(Debug)]
pub(super) struct State {
	packet_identifiers: super::PacketIdentifiers,

	at_least_once_waiting_to_be_acked:
		std::collections::BTreeMap<crate::proto::PacketIdentifier, (futures::sync::oneshot::Sender<()>, crate::proto::Packet)>,
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
			Some(crate::proto::Packet::PubAck { packet_identifier }) => match self.at_least_once_waiting_to_be_acked.remove(&packet_identifier) {
				Some((ack_sender, _)) => {
					self.packet_identifiers.discard(packet_identifier);

					match ack_sender.send(()) {
						Ok(()) => (),
						Err(()) => log::debug!("could not send ack for publish request because ack receiver has been dropped"),
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
						Err(()) => log::debug!("could not send ack for publish request because ack receiver has been dropped"),
					}
				},

				crate::proto::QoS::AtLeastOnce => {
					let packet_identifier = self.packet_identifiers.reserve();

					let packet = crate::proto::Packet::Publish {
						packet_identifier_dup_qos: crate::proto::PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
						retain: publication.retain,
						topic_name: publication.topic_name.clone(),
						payload: publication.payload.clone(),
					};

					self.at_least_once_waiting_to_be_acked.insert(packet_identifier, (ack_sender, crate::proto::Packet::Publish {
						packet_identifier_dup_qos: crate::proto::PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, true),
						retain: publication.retain,
						topic_name: publication.topic_name,
						payload: publication.payload,
					}));

					packets_waiting_to_be_sent.push(packet);
				},

				crate::proto::QoS::ExactlyOnce => unimplemented!(),
			}
		}

		Ok((packets_waiting_to_be_sent, publications_received))
	}

	pub (super) fn new_connection<'a>(&'a mut self) -> impl Iterator<Item = crate::proto::Packet> + 'a {
		self.at_least_once_waiting_to_be_acked.values()
		.map(|(_, packet)| packet.clone())
	}
}

impl Default for State {
	fn default() -> Self {
		State {
			packet_identifiers: Default::default(),

			at_least_once_waiting_to_be_acked: Default::default(),
		}
	}
}
