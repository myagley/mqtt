use itertools::Itertools;

#[derive(Debug)]
pub(super) struct State {
	packet_identifiers: super::PacketIdentifiers,

	subscriptions: std::collections::HashMap<String, crate::proto::QoS>,

	subscriptions_waiting_to_be_acked: std::collections::HashMap<crate::proto::PacketIdentifier, Vec<crate::proto::SubscribeTo>>,
	unsubscriptions_waiting_to_be_acked: std::collections::HashMap<crate::proto::PacketIdentifier, Vec<String>>,
}

impl State {
	pub(super) fn poll(
		&mut self,
		packet: &mut Option<crate::proto::Packet>,
		subscription_updates_waiting_to_be_sent: Vec<super::SubscriptionUpdate>,
	) -> std::io::Result<Vec<crate::proto::Packet>> {
		match packet.take() {
			Some(crate::proto::Packet::SubAck { packet_identifier, qos }) => {
				let subscriptions =
					self.subscriptions_waiting_to_be_acked.remove(&packet_identifier)
					.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "received SUBACK for a SUBSCRIBE we never sent"))?;

				self.packet_identifiers.discard(packet_identifier);

				if subscriptions.len() != qos.len() {
					return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "SUBACK does not contain correct number of qos"));
				}

				for (subscribe_to, qos) in subscriptions.into_iter().zip(qos) {
					if qos < subscribe_to.qos {
						return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!(
							"{} requested QoS {:?} but SUBACK contains QoS {:?}",
							subscribe_to.topic_filter,
							subscribe_to.qos,
							qos,
						)));
					}

					log::debug!("Subscribed to {} with {:?}", subscribe_to.topic_filter, qos);
					self.subscriptions.insert(subscribe_to.topic_filter, qos);
				}
			},

			Some(crate::proto::Packet::UnsubAck { packet_identifier }) => {
				let unsubscriptions =
					self.unsubscriptions_waiting_to_be_acked.remove(&packet_identifier)
					.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "received UNSUBACK for a UNSUBSCRIBE we never sent"))?;

				self.packet_identifiers.discard(packet_identifier);

				for unsubscribe_from in unsubscriptions {
					log::debug!("Unsubscribed from {}", unsubscribe_from);
					self.subscriptions.remove(&unsubscribe_from);
				}
			},

			other => *packet = other,
		}

		let mut packets_waiting_to_be_sent = vec![];
		// subscription_updates_waiting_to_be_sent may contain Subscribe and Unsubscribe in arbitrary order, so we have to partition them into
		// a group of Subscribe and a group of Unsubscribe.
		//
		// But the client have have unsubscribed to an earlier subscription, and both the Subscribe and the later Unsubscribe might be in this list.
		// Similarly, the client have have re-subscribed after unsubscribing, and both the Unsubscribe and the later Subscribe might be in this list.
		//
		// So we cannot just make a group of all Subscribes, send that packet, then make a group of all Unsubscribes, then send that packet.
		// Instead, we have to respect the ordering of Subscribes with Unsubscribes.
		// So we make groups of *consecutive* Subscribes and *consecutive* Unsubscribes, and construct one packet for each such group.
		for (_, subscription_updates) in &subscription_updates_waiting_to_be_sent.into_iter().group_by(std::mem::discriminant) {
			let mut subscription_updates = subscription_updates.peekable();

			let packet_identifier = self.packet_identifiers.reserve();

			let mut subscriptions_waiting_to_be_acked = vec![];
			let mut unsubscriptions_waiting_to_be_acked = vec![];

			if let Some(super::SubscriptionUpdate::Subscribe(_)) = subscription_updates.peek() {
				for subscription_update in subscription_updates {
					if let super::SubscriptionUpdate::Subscribe(subscribe_to) = subscription_update {
						subscriptions_waiting_to_be_acked.push(subscribe_to);
					}
					else {
						unreachable!();
					}
				}

				self.subscriptions_waiting_to_be_acked.insert(packet_identifier, subscriptions_waiting_to_be_acked.clone());

				packets_waiting_to_be_sent.push(crate::proto::Packet::Subscribe {
					packet_identifier,
					subscribe_to: subscriptions_waiting_to_be_acked,
				});
			}
			else {
				for subscription_update in subscription_updates {
					if let super::SubscriptionUpdate::Unsubscribe(unsubscribe_from) = subscription_update {
						unsubscriptions_waiting_to_be_acked.push(unsubscribe_from);
					}
					else {
						unreachable!();
					}
				}

				self.unsubscriptions_waiting_to_be_acked.insert(packet_identifier, unsubscriptions_waiting_to_be_acked.clone());

				packets_waiting_to_be_sent.push(crate::proto::Packet::Unsubscribe {
					packet_identifier,
					unsubscribe_from: unsubscriptions_waiting_to_be_acked,
				});
			}
		}

		Ok(packets_waiting_to_be_sent)
	}

	pub(super) fn new_connection(&mut self, reset_session: bool) -> impl Iterator<Item = crate::proto::Packet> {
		if reset_session {
			self.packet_identifiers = Default::default();

			let mut subscriptions = std::mem::replace(&mut self.subscriptions, Default::default());
			let subscriptions_waiting_to_be_acked = std::mem::replace(&mut self.subscriptions_waiting_to_be_acked, Default::default());
			let unsubscriptions_waiting_to_be_acked = std::mem::replace(&mut self.unsubscriptions_waiting_to_be_acked, Default::default());

			// Apply all pending (ie unacked) changes to the set of subscriptions, in order of packet identifier
			let pending_changes: std::collections::BTreeMap<_, _> =
				subscriptions_waiting_to_be_acked.into_iter().map(|(packet_identifier, subscribe_to)| {
					let subscription_updates: Vec<_> =
						subscribe_to.into_iter()
						.map(super::SubscriptionUpdate::Subscribe)
						.collect();
					(packet_identifier, subscription_updates)
				})
				.chain(unsubscriptions_waiting_to_be_acked.into_iter().map(|(packet_identifier, unsubscribe_from)| {
					let subscription_updates: Vec<_> =
						unsubscribe_from.into_iter()
						.map(super::SubscriptionUpdate::Unsubscribe)
						.collect();
					(packet_identifier, subscription_updates)
				}))
				.collect();
			for (_, pending_changes) in pending_changes {
				for pending_change in pending_changes {
					match pending_change {
						super::SubscriptionUpdate::Subscribe(crate::proto::SubscribeTo { topic_filter, qos }) => subscriptions.insert(topic_filter, qos),
						super::SubscriptionUpdate::Unsubscribe(topic_filter) => subscriptions.remove(&topic_filter),
					};
				}
			}

			// Generate SUBSCRIBE packets for the final set of subscriptions
			let subscriptions_waiting_to_be_acked: Vec<_> =
				subscriptions.into_iter()
				.map(|(topic_filter, qos)| crate::proto::SubscribeTo {
					topic_filter,
					qos,
				})
				.collect();

			if subscriptions_waiting_to_be_acked.is_empty() {
				NewConnectionIter::Empty
			}
			else {
				let packet_identifier = self.packet_identifiers.reserve();
				self.subscriptions_waiting_to_be_acked.insert(packet_identifier, subscriptions_waiting_to_be_acked.clone());

				NewConnectionIter::Single(std::iter::once(crate::proto::Packet::Subscribe {
					packet_identifier,
					subscribe_to: subscriptions_waiting_to_be_acked,
				}))
			}
		}
		else {
			// Re-create all pending (ie unacked) changes to the set of subscriptions, in order of packet identifier
			let mut unacked_packets: Vec<_> =
				self.subscriptions_waiting_to_be_acked.iter()
				.map(|(packet_identifier, subscribe_to)| crate::proto::Packet::Subscribe {
					packet_identifier: *packet_identifier,
					subscribe_to: subscribe_to.clone(),
				})
				.chain(
					self.unsubscriptions_waiting_to_be_acked.iter()
					.map(|(packet_identifier, unsubscribe_from)| crate::proto::Packet::Unsubscribe {
						packet_identifier: *packet_identifier,
						unsubscribe_from: unsubscribe_from.clone(),
					}))
				.collect();
			unacked_packets.sort_by(|packet1, packet2| match (packet1, packet2) {
				(
					crate::proto::Packet::Subscribe { packet_identifier: packet_identifier1, .. },
					crate::proto::Packet::Subscribe { packet_identifier: packet_identifier2, .. },
				) =>
					packet_identifier1.cmp(packet_identifier2),

				_ => unreachable!(),
			});

			NewConnectionIter::Multiple(unacked_packets.into_iter())
		}
	}
}

impl Default for State {
	fn default() -> Self {
		State {
			packet_identifiers: Default::default(),

			subscriptions: Default::default(),

			subscriptions_waiting_to_be_acked: Default::default(),
			unsubscriptions_waiting_to_be_acked: Default::default(),
		}
	}
}

#[derive(Debug)]
enum NewConnectionIter {
	Empty,
	Single(std::iter::Once<crate::proto::Packet>),
	Multiple(std::vec::IntoIter<crate::proto::Packet>),
}

impl Iterator for NewConnectionIter {
	type Item = crate::proto::Packet;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			NewConnectionIter::Empty => None,
			NewConnectionIter::Single(packet) => packet.next(),
			NewConnectionIter::Multiple(packets) => packets.next(),
		}
	}
}
