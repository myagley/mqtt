#[derive(Debug)]
pub(super) struct State {
	packet_identifiers: super::PacketIdentifiers,

	previously_seen_publish_packets: PreviouslySeenPackets,
	at_least_once_waiting_to_be_acked:
		std::collections::BTreeMap<crate::proto::PacketIdentifier, (futures::sync::oneshot::Sender<()>, crate::proto::Packet)>,
}

impl State {
	pub(super) fn poll(
		&mut self,
		packet: &mut Option<crate::proto::Packet>,
		publish_requests_waiting_to_be_sent: Vec<super::PublishRequest>,
	) -> (Vec<crate::proto::Packet>, Vec<crate::Publication>) {
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
				// If the server sends a duplicate PUBLISH packet and its identifier is in the previously-seen set,
				// then we assume the server did not receive the ack we sent the last time we received this packet.
				// So we ack it again but *don't* forward it to the client.
				let previously_seen_packet_identifier = match packet_identifier_dup_qos {
					crate::proto::PacketIdentifierDupQoS::AtMostOnce =>
						None,

					crate::proto::PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, dup) =>
						if dup && self.previously_seen_publish_packets.previously_seen(packet_identifier) {
							log::debug!("ignoring duplicate PUBLISH packet {}", packet_identifier);
							Some(packet_identifier)
						}
						else {
							None
						},

					crate::proto::PacketIdentifierDupQoS::ExactlyOnce(_, _) =>
						unimplemented!(),
				};

				if let Some(packet_identifier) = previously_seen_packet_identifier {
					packets_waiting_to_be_sent.push(crate::proto::Packet::PubAck {
						packet_identifier,
					});
				}
				else {
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

		(packets_waiting_to_be_sent, publications_received)
	}

	pub (super) fn new_connection<'a>(&'a mut self) -> impl Iterator<Item = crate::proto::Packet> + 'a {
		// Do not reset self.previously_seen_publish_packets here, *even if* the session has been reset by the server.
		//
		// If the server does send a new PUBLISH packet with a packet identifier that was previously marked as seen,
		// the packet would not be marked as a duplicate since the server should have cleared its session,
		// so it doesn't matter if the set was cleared or not.
		//
		// If the server *does* send a duplicate PUBLISH packet with an already-seen identifier, say because
		// it hasn't reset its session state despite claiming to do so, then we'll be *right* to ack it without
		// forwarding it to the client.
		//
		// So it's correct to not clear the set.

		self.at_least_once_waiting_to_be_acked.values()
		.map(|(_, packet)| packet.clone())
	}
}

impl Default for State {
	fn default() -> Self {
		State {
			packet_identifiers: Default::default(),

			previously_seen_publish_packets: Default::default(),
			at_least_once_waiting_to_be_acked: Default::default(),
		}
	}
}

struct PreviouslySeenPackets(Box<[u8; PreviouslySeenPackets::SIZE]>);

impl PreviouslySeenPackets {
	/// Size of a bitset for every packet identifier
	///
	/// Packet identifiers are u16's, and there are 8 bits to a byte, so the size
	/// = number of u16's / 8
	/// = pow(2, number of bits in a u16) / 8
	/// = pow(2, 16) / 8
	const SIZE: usize = 8192;

	/// Returns `true` if this packet was seen before, `false` if it was not
	fn previously_seen(&mut self, packet_identifier: crate::proto::PacketIdentifier) -> bool {
		let packet_identifier = packet_identifier.get();
		let (block, offset) = ((packet_identifier / 8) as usize, (packet_identifier % 8) as usize);
		let mask = 1 << offset;
		let is_set = (self.0[block] & mask) != 0;
		self.0[block] |= mask;
		is_set
	}
}

impl std::fmt::Debug for PreviouslySeenPackets {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		f.write_str("PreviouslySeenPackets")
	}
}

impl Default for PreviouslySeenPackets {
	fn default() -> Self {
		PreviouslySeenPackets(Box::new([0; PreviouslySeenPackets::SIZE]))
	}
}
