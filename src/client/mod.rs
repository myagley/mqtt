use futures::{ Future, Sink, Stream };

mod connect;
mod ping;
mod publish;
mod subscriptions;

pub use self::publish::{ Publication, PublishError, PublishHandle };
pub use self::subscriptions::{ UpdateSubscriptionError, UpdateSubscriptionHandle };

/// An MQTT client
#[derive(Debug)]
pub struct Client<IoS> where IoS: IoSource {
	client_id: crate::proto::ClientId,
	username: Option<String>,
	password: Option<String>,
	keep_alive: std::time::Duration,

	packet_identifiers: PacketIdentifiers,

	connect: self::connect::Connect<IoS>,
	ping: self::ping::State,
	publish: self::publish::State,
	subscriptions: self::subscriptions::State,

	/// Packets waiting to be written to the underlying `Framed`
	packets_waiting_to_be_sent: std::collections::VecDeque<crate::proto::Packet>,
}

impl<IoS> Client<IoS> where IoS: IoSource {
	/// Create a new client with the given parameters
	///
	/// * `client_id`
	///
	///     If set, this ID will be used to start a new clean session with the server. On subsequent re-connects, the ID will be re-used.
	///     Otherwise, the client will use a server-generated ID for each new connection.
	///
	/// * `username`, `password`
	///
	///     Optional credentials for the server.
	///
	/// * `io_source`
	///
	///     The MQTT protocol is layered onto the I/O object returned by this source.
	///
	/// * `max_reconnect_back_off`
	///
	///     Every connection failure will double the back-off period, to a maximum of this value.
	///
	/// * `keep_alive`
	///
	///     The keep-alive time advertised to the server. The client will ping the server at half this interval.
	pub fn new(
		client_id: Option<String>,
		username: Option<String>,
		password: Option<String>,
		io_source: IoS,
		max_reconnect_back_off: std::time::Duration,
		keep_alive: std::time::Duration,
	) -> Self {
		let client_id = match client_id {
			Some(id) => crate::proto::ClientId::IdWithCleanSession(id),
			None => crate::proto::ClientId::ServerGenerated,
		};

		Client {
			client_id,
			username,
			password,
			keep_alive,

			packet_identifiers: Default::default(),

			connect: self::connect::Connect::new(io_source, max_reconnect_back_off),
			ping: self::ping::State::BeginWaitingForNextPing,
			publish: Default::default(),
			subscriptions: Default::default(),

			packets_waiting_to_be_sent: Default::default(),
		}
	}
}

impl<IoS> Client<IoS> where IoS: IoSource {
	/// Returns a handle that can be used to publish messages to the server
	pub fn publish_handle(&self) -> PublishHandle {
		self.publish.publish_handle()
	}

	/// Returns a handle that can be used to update subscriptions
	pub fn update_subscription_handle(&self) -> UpdateSubscriptionHandle {
		self.subscriptions.update_subscription_handle()
	}
}

impl<IoS> Stream for Client<IoS> where IoS: IoSource, <<IoS as IoSource>::Future as Future>::Error: std::fmt::Display {
	type Item = Vec<crate::ReceivedPublication>;
	type Error = Error;

	fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
		loop {
			let self::connect::Connected { framed, new_connection, reset_session } = match self.connect.poll(
				self.username.as_ref().map(AsRef::as_ref),
				self.password.as_ref().map(AsRef::as_ref),
				&mut self.client_id,
				self.keep_alive,
			) {
				Ok(futures::Async::Ready(framed)) => framed,
				Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
				Err(()) => unreachable!(),
			};

			if new_connection {
				log::debug!("New connection established");

				self.packets_waiting_to_be_sent = Default::default();

				self.ping.new_connection();

				self.packets_waiting_to_be_sent.extend(self.publish.new_connection(reset_session, &mut self.packet_identifiers));

				self.packets_waiting_to_be_sent.extend(self.subscriptions.new_connection(reset_session, &mut self.packet_identifiers));
			}

			match client_poll(
				framed,
				self.keep_alive,
				&mut self.packets_waiting_to_be_sent,
				&mut self.packet_identifiers,
				&mut self.ping,
				&mut self.publish,
				&mut self.subscriptions,
			) {
				Ok(futures::Async::Ready(result)) => break Ok(futures::Async::Ready(Some(result))),
				Ok(futures::Async::NotReady) => break Ok(futures::Async::NotReady),
				Err(err) =>
					if err.is_user_error() {
						break Err(err);
					}
					else {
						log::warn!("client will reconnect because of error: {}", err);

						// Ensure clean session
						//
						// DEVNOTE: subscriptions::State relies on the fact that the session is reset here.
						// Update that if this ever changes.
						self.client_id = match std::mem::replace(&mut self.client_id, crate::proto::ClientId::ServerGenerated) {
							id @ crate::proto::ClientId::ServerGenerated |
							id @ crate::proto::ClientId::IdWithCleanSession(_) => id,
							crate::proto::ClientId::IdWithExistingSession(id) => crate::proto::ClientId::IdWithCleanSession(id),
						};

						self.connect.reconnect();
					},
			}
		}
	}
}

/// This trait provides an I/O object that a [`Client`] can use.
///
/// The trait is automatically implemented for all [`FnMut`] that return a connection future.
pub trait IoSource {
	/// The I/O object
	type Io: tokio::io::AsyncRead + tokio::io::AsyncWrite;

	/// The connection future
	type Future: Future<Item = Self::Io>;

	/// Attempts the connection and returns a [`Future`] that resolves when the connection succeeds
	fn connect(&mut self) -> Self::Future;
}

impl<F, A> IoSource for F
where
	F: FnMut() -> A,
	A: Future,
	<A as Future>::Item: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
	type Io = <A as Future>::Item;
	type Future = A;

	fn connect(&mut self) -> Self::Future {
		(self)()
	}
}

/// A message that was received from the server
#[derive(Debug)]
pub struct ReceivedPublication {
	pub topic_name: String,
	pub dup: bool,
	pub qos: crate::proto::QoS,
	pub payload: Vec<u8>,
}

fn client_poll<S>(
	framed: &mut crate::logging_framed::LoggingFramed<S>,
	keep_alive: std::time::Duration,
	packets_waiting_to_be_sent: &mut std::collections::VecDeque<crate::proto::Packet>,
	packet_identifiers: &mut PacketIdentifiers,
	ping: &mut self::ping::State,
	publish: &mut self::publish::State,
	subscriptions: &mut self::subscriptions::State,
) -> futures::Poll<Vec<crate::ReceivedPublication>, Error>
where
	S: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
	let mut publications_received = vec![];

	loop {
		// Begin sending any packets waiting to be sent
		while let Some(packet) = packets_waiting_to_be_sent.pop_front() {
			match framed.start_send(packet).map_err(Error::EncodePacket)? {
				futures::AsyncSink::Ready => (),

				futures::AsyncSink::NotReady(packet) => {
					packets_waiting_to_be_sent.push_front(packet);
					break;
				},
			}
		}

		// Finish sending any packets waiting to be sent.
		//
		// We don't care whether this returns Async::NotReady or Ready.
		let _ = framed.poll_complete().map_err(Error::EncodePacket)?;

		let mut continue_loop = false;

		let mut packet = match framed.poll().map_err(Error::DecodePacket)? {
			futures::Async::Ready(Some(packet)) => {
				// May have more packets after this one, so keep looping
				continue_loop = true;
				Some(packet)
			},
			futures::Async::Ready(None) => return Err(Error::ServerClosedConnection),
			futures::Async::NotReady => None,
		};

		let mut new_packets_to_be_sent = vec![];


		// Ping
		match ping.poll(&mut packet, keep_alive)? {
			futures::Async::Ready(packet) => new_packets_to_be_sent.push(packet),
			futures::Async::NotReady => (),
		}

		// Publish
		let (new_publish_packets, new_publication_received) = publish.poll(
			&mut packet,
			packet_identifiers,
		)?;
		new_packets_to_be_sent.extend(new_publish_packets);
		if let Some(new_publication_received) = new_publication_received {
			publications_received.push(new_publication_received);
		}

		// Subscriptions
		new_packets_to_be_sent.extend(subscriptions.poll(
			&mut packet,
			packet_identifiers,
		)?);


		assert!(packet.is_none(), "unconsumed packet");

		if !new_packets_to_be_sent.is_empty() {
			// Have new packets to send, so keep looping
			continue_loop = true;
			packets_waiting_to_be_sent.extend(new_packets_to_be_sent);
		}

		if !continue_loop {
			if publications_received.is_empty() {
				break Ok(futures::Async::NotReady);
			}
			else {
				break Ok(futures::Async::Ready(publications_received));
			}
		}
	}
}

struct PacketIdentifiers {
	in_use: Box<[u64; PacketIdentifiers::SIZE]>,
	previous: crate::proto::PacketIdentifier,
}

impl PacketIdentifiers {
	/// Size of a bitset for every packet identifier
	///
	/// Packet identifiers are u16's, and there are 64 bits to a u64, so the number of u64's required
	/// = number of u16's / 64
	/// = pow(2, number of bits in a u16) / 64
	/// = pow(2, 16) / 64
	const SIZE: usize = 1024;

	fn reserve(&mut self) -> Result<crate::proto::PacketIdentifier, Error> {
		let start = self.previous;
		let mut current = start;

		loop {
			current += 1;

			let (block, mask) = self.entry(current);
			if (*block & mask) == 0 {
				*block |= mask;
				self.previous = current;
				return Ok(current);
			}

			if current == start {
				return Err(Error::PacketIdentifiersExhausted);
			}
		}
	}

	fn discard(&mut self, packet_identifier: crate::proto::PacketIdentifier) {
		let (block, mask) = self.entry(packet_identifier);
		*block &= !mask;
	}

	fn entry(&mut self, packet_identifier: crate::proto::PacketIdentifier) -> (&mut u64, u64) {
		let packet_identifier = packet_identifier.get();
		let (block, offset) = ((packet_identifier / 64) as usize, (packet_identifier % 64) as usize);
		(&mut self.in_use[block], 1 << offset)
	}
}

impl std::fmt::Debug for PacketIdentifiers {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		f.debug_struct("PacketIdentifiers").field("previous", &self.previous).finish()
	}
}

impl Default for PacketIdentifiers {
	fn default() -> Self {
		PacketIdentifiers {
			in_use: Box::new([0; PacketIdentifiers::SIZE]),
			previous: crate::proto::PacketIdentifier::max_value(),
		}
	}
}

#[derive(Debug)]
pub enum Error {
	DecodePacket(crate::proto::DecodeError),
	DuplicateExactlyOncePublishPacketNotMarkedDuplicate(crate::proto::PacketIdentifier),
	EncodePacket(crate::proto::EncodeError),
	PacketIdentifiersExhausted,
	PingTimer(tokio::timer::Error),
	ServerClosedConnection,
	SubAckDoesNotContainEnoughQoS(crate::proto::PacketIdentifier, usize, usize),
	SubscriptionDowngraded(String, crate::proto::QoS, crate::proto::QoS),
	SubscriptionFailed,
	UnexpectedSubAck(crate::proto::PacketIdentifier, UnexpectedSubUnsubAckReason),
	UnexpectedUnsubAck(crate::proto::PacketIdentifier, UnexpectedSubUnsubAckReason),
}

#[derive(Clone, Copy, Debug)]
pub enum UnexpectedSubUnsubAckReason {
	DidNotExpect,
	Expected(crate::proto::PacketIdentifier),
	ExpectedSubAck(crate::proto::PacketIdentifier),
	ExpectedUnsubAck(crate::proto::PacketIdentifier),
}

impl Error {
	fn is_user_error(&self) -> bool {
		match self {
			Error::EncodePacket(err) => err.is_user_error(),
			_ => false,
		}
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			Error::DecodePacket(err) =>
				write!(f, "could not decode packet: {}", err),

			Error::DuplicateExactlyOncePublishPacketNotMarkedDuplicate(packet_identifier) =>
				write!(
					f,
					"server sent a new ExactlyOnce PUBLISH packet {} with the same packet identifier as another unacknowledged ExactlyOnce PUBLISH packet",
					packet_identifier,
				),

			Error::EncodePacket(err) =>
				write!(f, "could not encode packet: {}", err),

			Error::PacketIdentifiersExhausted =>
				write!(f, "all packet identifiers exhausted"),

			Error::PingTimer(err) =>
				write!(f, "ping timer failed: {}", err),

			Error::ServerClosedConnection =>
				write!(f, "connection closed by server"),

			Error::SubAckDoesNotContainEnoughQoS(packet_identifier, expected, actual) =>
				write!(f, "Expected SUBACK {} to contain {} QoS's but it actually contained {}", packet_identifier, expected, actual),

			Error::SubscriptionDowngraded(topic_name, expected, actual) =>
				write!(f, "Server downgraded subscription for topic filter {:?} with QoS {:?} to {:?}", topic_name, expected, actual),

			Error::SubscriptionFailed =>
				write!(f, "Server rejected one or more subscriptions"),

			Error::UnexpectedSubAck(packet_identifier, reason) =>
				write!(f, "received SUBACK {} but {}", packet_identifier, reason),

			Error::UnexpectedUnsubAck(packet_identifier, reason) =>
				write!(f, "received UNSUBACK {} but {}", packet_identifier, reason),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(std::error::Error + 'static)> {
		#[allow(clippy::match_same_arms)]
		match self {
			Error::DecodePacket(err) => Some(err),
			Error::DuplicateExactlyOncePublishPacketNotMarkedDuplicate(_) => None,
			Error::EncodePacket(err) => Some(err),
			Error::PacketIdentifiersExhausted => None,
			Error::PingTimer(err) => Some(err),
			Error::ServerClosedConnection => None,
			Error::SubAckDoesNotContainEnoughQoS(_, _, _) => None,
			Error::SubscriptionDowngraded(_, _, _) => None,
			Error::SubscriptionFailed => None,
			Error::UnexpectedSubAck(_, _) => None,
			Error::UnexpectedUnsubAck(_, _) => None,
		}
	}
}

impl std::fmt::Display for UnexpectedSubUnsubAckReason {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			UnexpectedSubUnsubAckReason::DidNotExpect => write!(f, "did not expect it"),
			UnexpectedSubUnsubAckReason::Expected(packet_identifier) => write!(f, "expected {}", packet_identifier),
			UnexpectedSubUnsubAckReason::ExpectedSubAck(packet_identifier) => write!(f, "expected SUBACK {}", packet_identifier),
			UnexpectedSubUnsubAckReason::ExpectedUnsubAck(packet_identifier) => write!(f, "expected UNSUBACK {}", packet_identifier),
		}
	}
}
