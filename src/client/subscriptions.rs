#[derive(Debug)]
pub(super) struct State {
	subscriptions: std::collections::HashMap<String, crate::proto::QoS>,

	subscription_updates_waiting_to_be_acked: std::collections::VecDeque<(crate::proto::PacketIdentifier, BatchedSubscriptionUpdate)>,
}

impl State {
	pub(super) fn poll(
		&mut self,
		packet: &mut Option<crate::proto::Packet>,
		subscription_updates_waiting_to_be_sent: &mut std::collections::VecDeque<super::SubscriptionUpdate>,
		packet_identifiers: &mut super::PacketIdentifiers,
	) -> Result<Vec<crate::proto::Packet>, super::Error> {
		match packet.take() {
			Some(crate::proto::Packet::SubAck { packet_identifier, qos }) => match self.subscription_updates_waiting_to_be_acked.pop_front() {
				Some((packet_identifier_waiting_to_be_acked, BatchedSubscriptionUpdate::Subscribe(subscribe_to))) => {
					if packet_identifier != packet_identifier_waiting_to_be_acked {
						self.subscription_updates_waiting_to_be_acked.push_front((
							packet_identifier_waiting_to_be_acked,
							BatchedSubscriptionUpdate::Subscribe(subscribe_to),
						));
						return Err(super::Error::UnexpectedSubAck(
							packet_identifier,
							super::UnexpectedSubUnsubAckReason::Expected(packet_identifier_waiting_to_be_acked),
						));
					}

					if subscribe_to.len() != qos.len() {
						let expected = subscribe_to.len();
						self.subscription_updates_waiting_to_be_acked.push_front((
							packet_identifier_waiting_to_be_acked,
							BatchedSubscriptionUpdate::Subscribe(subscribe_to),
						));
						return Err(super::Error::SubAckDoesNotContainEnoughQoS(packet_identifier, expected, qos.len()));
					}

					packet_identifiers.discard(packet_identifier);

					// We can't put subscribe_to back into self.subscription_updates_waiting_to_be_acked within the below loop
					// since we would've partially consumed it.
					// Instead, if there's an error, we'll update self.subscriptions anyway with the expected QoS, and set the error to be returned here.
					// The error will reset the session and resend the subscription requests, including these that didn't match the expected QoS,
					// so pretending the subscription succeeded does no harm.
					let mut err = None;
					for (crate::proto::SubscribeTo { topic_filter, qos: expected_qos }, qos) in subscribe_to.into_iter().zip(qos) {
						match qos {
							crate::proto::SubAckQos::Success(actual_qos) =>
								if actual_qos >= expected_qos {
									log::debug!("Subscribed to {} with {:?}", topic_filter, actual_qos);
									self.subscriptions.insert(topic_filter, actual_qos);
								}
								else {
									if err.is_none() {
										err = Some(super::Error::SubscriptionDowngraded(topic_filter.clone(), expected_qos, actual_qos));
									}

									self.subscriptions.insert(topic_filter, expected_qos);
								},

							crate::proto::SubAckQos::Failure => {
								if err.is_none() {
									err = Some(super::Error::SubscriptionFailed);
								}

								self.subscriptions.insert(topic_filter, expected_qos);
							},
						}
					}

					if let Some(err) = err {
						return Err(err);
					}
				},

				Some((packet_identifier_waiting_to_be_acked, unsubscribe @ BatchedSubscriptionUpdate::Unsubscribe(_))) => {
					self.subscription_updates_waiting_to_be_acked.push_front((packet_identifier, unsubscribe));
					return Err(super::Error::UnexpectedSubAck(
						packet_identifier,
						super::UnexpectedSubUnsubAckReason::ExpectedUnsubAck(packet_identifier_waiting_to_be_acked),
					));
				},

				None =>
					return Err(super::Error::UnexpectedSubAck(packet_identifier, super::UnexpectedSubUnsubAckReason::DidNotExpect)),
			},

			Some(crate::proto::Packet::UnsubAck { packet_identifier }) => match self.subscription_updates_waiting_to_be_acked.pop_front() {
				Some((packet_identifier_waiting_to_be_acked, BatchedSubscriptionUpdate::Unsubscribe(unsubscribe_from))) => {
					if packet_identifier != packet_identifier_waiting_to_be_acked {
						self.subscription_updates_waiting_to_be_acked.push_front((
							packet_identifier_waiting_to_be_acked,
							BatchedSubscriptionUpdate::Unsubscribe(unsubscribe_from),
						));
						return Err(super::Error::UnexpectedUnsubAck(
							packet_identifier,
							super::UnexpectedSubUnsubAckReason::Expected(packet_identifier_waiting_to_be_acked),
						));
					}

					packet_identifiers.discard(packet_identifier);

					for topic_filter in unsubscribe_from {
						log::debug!("Unsubscribed from {}", topic_filter);
						self.subscriptions.remove(&topic_filter);
					}
				},

				Some((packet_identifier_waiting_to_be_acked, subscribe @ BatchedSubscriptionUpdate::Subscribe(_))) => {
					self.subscription_updates_waiting_to_be_acked.push_front((packet_identifier_waiting_to_be_acked, subscribe));
					return Err(super::Error::UnexpectedUnsubAck(
						packet_identifier,
						super::UnexpectedSubUnsubAckReason::ExpectedSubAck(packet_identifier_waiting_to_be_acked),
					));
				},

				None =>
					return Err(super::Error::UnexpectedUnsubAck(packet_identifier, super::UnexpectedSubUnsubAckReason::DidNotExpect)),
			},

			other => *packet = other,
		}


		// Rather than send individual SUBSCRIBE and UNSUBSCRIBE packets for each update, we can send multiple updates in the same packet.
		// subscription_updates_waiting_to_be_sent may contain Subscribe and Unsubscribe in arbitrary order, so we have to partition them into
		// a group of Subscribe and a group of Unsubscribe.
		//
		// But the client have have unsubscribed to an earlier subscription, and both the Subscribe and the later Unsubscribe might be in this list.
		// Similarly, the client have have re-subscribed after unsubscribing, and both the Unsubscribe and the later Subscribe might be in this list.
		//
		// So we cannot just make a group of all Subscribes, send that packet, then make a group of all Unsubscribes, then send that packet.
		// Instead, we have to respect the ordering of Subscribes with Unsubscribes.
		// So we make groups of *consecutive* Subscribes and *consecutive* Unsubscribes, and construct one packet for each such group.

		let mut packets_waiting_to_be_sent = vec![];

		while let Some(subscription_update) = subscription_updates_waiting_to_be_sent.pop_front() {
			let packet_identifier = match packet_identifiers.reserve() {
				Ok(packet_identifier) => packet_identifier,
				Err(err) => {
					subscription_updates_waiting_to_be_sent.push_front(subscription_update);
					return Err(err);
				},
			};

			match subscription_update {
				super::SubscriptionUpdate::Subscribe(subscribe_to) => {
					let mut subscriptions_waiting_to_be_acked = vec![subscribe_to];

					loop {
						match subscription_updates_waiting_to_be_sent.pop_front() {
							Some(super::SubscriptionUpdate::Subscribe(subscribe_to)) => subscriptions_waiting_to_be_acked.push(subscribe_to),
							Some(unsubscribe @ super::SubscriptionUpdate::Unsubscribe(_)) => {
								subscription_updates_waiting_to_be_sent.push_front(unsubscribe);
								break;
							},
							None => break,
						}
					}

					self.subscription_updates_waiting_to_be_acked.push_back((
						packet_identifier,
						BatchedSubscriptionUpdate::Subscribe(subscriptions_waiting_to_be_acked.clone()),
					));

					packets_waiting_to_be_sent.push(crate::proto::Packet::Subscribe {
						packet_identifier,
						subscribe_to: subscriptions_waiting_to_be_acked,
					});
				},

				super::SubscriptionUpdate::Unsubscribe(unsubscribe_from) => {
					let mut unsubscriptions_waiting_to_be_acked = vec![unsubscribe_from];

					loop {
						match subscription_updates_waiting_to_be_sent.pop_front() {
							Some(super::SubscriptionUpdate::Unsubscribe(unsubscribe_from)) => unsubscriptions_waiting_to_be_acked.push(unsubscribe_from),
							Some(subscribe @ super::SubscriptionUpdate::Subscribe(_)) => {
								subscription_updates_waiting_to_be_sent.push_front(subscribe);
								break;
							},
							None => break,
						}
					}

					self.subscription_updates_waiting_to_be_acked.push_back((
						packet_identifier,
						BatchedSubscriptionUpdate::Unsubscribe(unsubscriptions_waiting_to_be_acked.clone()),
					));

					packets_waiting_to_be_sent.push(crate::proto::Packet::Unsubscribe {
						packet_identifier,
						unsubscribe_from: unsubscriptions_waiting_to_be_acked,
					});
				},
			}
		}

		Ok(packets_waiting_to_be_sent)
	}

	pub(super) fn new_connection(
		&mut self,
		reset_session: bool,
		packet_identifiers: &mut super::PacketIdentifiers,
	) -> impl Iterator<Item = crate::proto::Packet> {
		if reset_session {
			let mut subscriptions = std::mem::replace(&mut self.subscriptions, Default::default());
			let subscription_updates_waiting_to_be_acked = std::mem::replace(&mut self.subscription_updates_waiting_to_be_acked, Default::default());

			// Apply all pending (ie unacked) changes to the set of subscriptions, in order that they were original requested
			for (packet_identifier, subscription_update_waiting_to_be_acked) in subscription_updates_waiting_to_be_acked {
				packet_identifiers.discard(packet_identifier);

				match subscription_update_waiting_to_be_acked {
					BatchedSubscriptionUpdate::Subscribe(subscribe_to) => {
						for crate::proto::SubscribeTo { topic_filter, qos } in subscribe_to {
							subscriptions.insert(topic_filter, qos);
						}
					},

					BatchedSubscriptionUpdate::Unsubscribe(unsubscribe_from) => {
						for topic_filter in unsubscribe_from {
							subscriptions.remove(&topic_filter);
						}
					},
				}
			}

			// Generate a SUBSCRIBE packet for the final set of subscriptions
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
				let packet_identifier = packet_identifiers.reserve().expect("reset session should have available packet identifiers");
				self.subscription_updates_waiting_to_be_acked.push_back((
					packet_identifier,
					BatchedSubscriptionUpdate::Subscribe(subscriptions_waiting_to_be_acked.clone()),
				));

				NewConnectionIter::Single(std::iter::once(crate::proto::Packet::Subscribe {
					packet_identifier,
					subscribe_to: subscriptions_waiting_to_be_acked,
				}))
			}
		}
		else {
			// Re-create all pending (ie unacked) changes to the set of subscriptions, in order of packet identifier
			let mut unacked_packets: Vec<_> =
				self.subscription_updates_waiting_to_be_acked.iter()
				.map(|(packet_identifier, subscription_update)| match subscription_update {
					BatchedSubscriptionUpdate::Subscribe(subscribe_to) =>
						crate::proto::Packet::Subscribe {
							packet_identifier: *packet_identifier,
							subscribe_to: subscribe_to.clone(),
						},

					BatchedSubscriptionUpdate::Unsubscribe(unsubscribe_from) =>
						crate::proto::Packet::Unsubscribe {
							packet_identifier: *packet_identifier,
							unsubscribe_from: unsubscribe_from.clone(),
						},
				})
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
			subscriptions: Default::default(),

			subscription_updates_waiting_to_be_acked: Default::default(),
		}
	}
}

#[derive(Debug)]
enum BatchedSubscriptionUpdate {
	Subscribe(Vec<crate::proto::SubscribeTo>),
	Unsubscribe(Vec<String>),
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
