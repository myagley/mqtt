use futures::{ Future, Sink, Stream };

mod connect;
mod ping;
mod publish;
mod subscriptions;

/// An MQTT client
#[derive(Debug)]
pub struct Client<IoS> where IoS: IoSource {
	client_id: crate::proto::ClientId,
	username: Option<String>,
	password: Option<String>,
	keep_alive: std::time::Duration,

	publish_request_send: futures::sync::mpsc::Sender<PublishRequest>,
	publish_request_recv: futures::sync::mpsc::Receiver<PublishRequest>,

	subscriptions_updated_send: futures::sync::mpsc::Sender<SubscriptionUpdate>,
	subscriptions_updated_recv: futures::sync::mpsc::Receiver<SubscriptionUpdate>,

	connect: self::connect::Connect<IoS>,
	ping: self::ping::State,
	publish: self::publish::State,
	subscriptions: self::subscriptions::State,

	/// Packets waiting to be written to the underlying `Framed`
	packets_waiting_to_be_sent: std::collections::VecDeque<crate::proto::Packet>,

	/// Dupes of packets that have been written to the underlying `Framed`, but not yet drained.
	/// That is, `poll_complete()` hasn't returned `Async::Ready(())` yet.
	///
	/// If the connection is re-established, it is unknown if these packets had been received by the server,
	/// so they will be sent again.
	packets_in_transit: Vec<crate::proto::Packet>,
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
	///
	/// * `max_pending_publish_requests`
	///
	///     The maximum number of pending publish requests that can be in flight at the same time.
	///
	/// * `max_pending_subscription_updates`
	///
	///     The maximum number of pending subscription updates (subscriptions and unsubscriptions) that can be in flight at the same time.
	pub fn new(
		client_id: Option<String>,
		username: Option<String>,
		password: Option<String>,
		io_source: IoS,
		max_reconnect_back_off: std::time::Duration,
		keep_alive: std::time::Duration,
		max_pending_publish_requests: usize,
		max_pending_subscription_updates: usize,
	) -> Self {
		let client_id = match client_id {
			Some(id) => crate::proto::ClientId::IdWithCleanSession(id),
			None => crate::proto::ClientId::ServerGenerated,
		};

		let (subscriptions_updated_send, subscriptions_updated_recv) = futures::sync::mpsc::channel(max_pending_subscription_updates);
		let (publish_request_send, publish_request_recv) = futures::sync::mpsc::channel(max_pending_publish_requests);

		Client {
			client_id,
			username,
			password,
			keep_alive,

			subscriptions_updated_send,
			subscriptions_updated_recv,

			publish_request_send,
			publish_request_recv,

			connect: self::connect::Connect::new(io_source, max_reconnect_back_off),
			ping: self::ping::State::BeginWaitingForNextPing,
			publish: Default::default(),
			subscriptions: Default::default(),

			packets_waiting_to_be_sent: Default::default(),
			packets_in_transit: Default::default(),
		}
	}
}

impl<IoS> Client<IoS> where IoS: IoSource {
	/// Returns a handle that can be used to publish messages to the server
	pub fn publish_handle(&self) -> PublishHandle {
		PublishHandle(self.publish_request_send.clone())
	}

	/// Returns a handle that can be used to update subscriptions
	pub fn update_subscription_handle(&self) -> UpdateSubscriptionHandle {
		UpdateSubscriptionHandle(self.subscriptions_updated_send.clone())
	}
}

impl<IoS> Stream for Client<IoS> where IoS: IoSource<Error = std::io::Error> {
	type Item = Vec<crate::Publication>;
	type Error = std::io::Error;

	fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
		loop {
			let self::connect::Connected { framed, new_connection, reset_session } = match self.connect.poll(
				self.username.as_ref().map(AsRef::as_ref),
				self.password.as_ref().map(AsRef::as_ref),
				&mut self.client_id,
				self.keep_alive,
			)? {
				futures::Async::Ready(framed) => framed,
				futures::Async::NotReady => return Ok(futures::Async::NotReady),
			};

			if new_connection {
				log::debug!("New connection established");

				let packets_in_transit = std::mem::replace(&mut self.packets_in_transit, Default::default());
				self.packets_waiting_to_be_sent = packets_in_transit.into_iter().collect();

				if reset_session {
					log::debug!("Resetting session...");

					self.ping.reset();

					if let Some(packet) = self.subscriptions.reset() {
						self.packets_waiting_to_be_sent.push_back(packet);
					}
				}
			}

			match client_poll(
				framed,
				self.keep_alive,
				&mut self.publish_request_recv,
				&mut self.subscriptions_updated_recv,
				&mut self.packets_waiting_to_be_sent,
				&mut self.packets_in_transit,
				&mut self.ping,
				&mut self.publish,
				&mut self.subscriptions,
			) {
				Ok(futures::Async::Ready(result)) => break Ok(futures::Async::Ready(Some(result))),
				Ok(futures::Async::NotReady) => break Ok(futures::Async::NotReady),
				Err(err) => {
					log::warn!("client will reconnect because of error: {}", err);
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

	/// The error returned by the connection future
	type Error: std::error::Error;

	/// The connection future
	type Future: Future<Item = Self::Io, Error = Self::Error>;

	/// Attempts the connection and returns a [`Future`] that resolves when the connection succeeds
	fn connect(&mut self) -> Self::Future;
}

impl<F, A> IoSource for F
where
	F: FnMut() -> A,
	A: Future,
	<A as Future>::Item: tokio::io::AsyncRead + tokio::io::AsyncWrite,
	<A as Future>::Error: std::error::Error,
{
	type Io = <A as Future>::Item;
	type Error = <A as Future>::Error;
	type Future = A;

	fn connect(&mut self) -> Self::Future {
		(self)()
	}
}

/// Used to update subscriptions
pub struct UpdateSubscriptionHandle(futures::sync::mpsc::Sender<SubscriptionUpdate>);

impl UpdateSubscriptionHandle {
	/// Subscribe to a topic with the given parameters
	pub fn subscribe(&mut self, subscribe_to: crate::proto::SubscribeTo) -> std::io::Result<()> {
		self.update_subscription(SubscriptionUpdate::Subscribe(subscribe_to))
	}

	/// Unsubscribe from the given topic
	pub fn unsubscribe(&mut self, unsubscribe_from: String) -> std::io::Result<()> {
		self.update_subscription(SubscriptionUpdate::Unsubscribe(unsubscribe_from))
	}

	fn update_subscription(&mut self, subscription_update: SubscriptionUpdate) -> std::io::Result<()> {
		match self.0.try_send(subscription_update) {
			Ok(()) => Ok(()),
			Err(err) =>
				if err.is_full() {
					Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "not ready"))
				}
				else {
					Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "client does not exist"))
				},
		}
	}
}

/// The kind of subscription update
#[derive(Clone, Debug)]
enum SubscriptionUpdate {
	Subscribe(crate::proto::SubscribeTo),
	Unsubscribe(String),
}

/// Used to publish messages to the server
pub struct PublishHandle(futures::sync::mpsc::Sender<PublishRequest>);

impl PublishHandle {
	/// Publish the given message to the server
	pub fn publish(&mut self, publication: Publication) -> std::io::Result<impl Future<Item = (), Error = std::io::Error>> {
		let (sender, receiver) = futures::sync::oneshot::channel();

		match self.0.try_send(PublishRequest { publication, ack_sender: sender }) {
			Ok(()) => Ok(
				receiver
				.then(|result| match result {
					Ok(()) => Ok(()),
					Err(futures::sync::oneshot::Canceled) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "sender dropped"))
				})),

			Err(err) =>
				// TODO: When replacing with real error type, return Publication back so that caller can retry
				if err.is_full() {
					Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "not ready"))
				}
				else {
					Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "client does not exist"))
				},
		}
	}
}

#[derive(Debug)]
struct PublishRequest {
	publication: Publication,
	ack_sender: futures::sync::oneshot::Sender<()>,
}

/// A message that can be published to the server
#[derive(Debug)]
pub struct Publication {
	pub topic_name: String,
	pub qos: crate::proto::QoS,
	pub retain: bool,
	pub payload: Vec<u8>,
}

fn client_poll<S>(
	framed: &mut crate::logging_framed::LoggingFramed<S>,
	keep_alive: std::time::Duration,
	publish_request_recv: &mut futures::sync::mpsc::Receiver<PublishRequest>,
	subscriptions_updated_recv: &mut futures::sync::mpsc::Receiver<SubscriptionUpdate>,
	packets_waiting_to_be_sent: &mut std::collections::VecDeque<crate::proto::Packet>,
	packets_in_transit: &mut Vec<crate::proto::Packet>,
	ping: &mut self::ping::State,
	publish: &mut self::publish::State,
	subscriptions: &mut self::subscriptions::State,

) -> futures::Poll<Vec<crate::Publication>, std::io::Error>
where
	S: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
	let mut publications_received = vec![];

	loop {
		// Begin sending any packets waiting to be sent
		while let Some(packet) = packets_waiting_to_be_sent.pop_front() {
			let dup = packet.dup();

			match framed.start_send(packet)? {
				futures::AsyncSink::Ready => if let Some(dup) = dup {
					packets_in_transit.push(dup);
				},

				futures::AsyncSink::NotReady(packet) => {
					packets_waiting_to_be_sent.push_front(packet);
					break;
				},
			}
		}

		// Finish sending any packets waiting to be sent.
		match framed.poll_complete()? {
			futures::Async::Ready(()) => packets_in_transit.clear(),
			futures::Async::NotReady => (),
		}

		let mut continue_loop = false;

		let mut packet = match framed.poll()? {
			futures::Async::Ready(Some(packet)) => {
				// May have more packets after this one, so keep looping
				continue_loop = true;
				Some(packet)
			},
			futures::Async::Ready(None) => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed by server")),
			futures::Async::NotReady => None,
		};

		let mut new_packets_to_be_sent = vec![];

		// ----
		// Ping
		// ----

		match ping.poll(&mut packet, keep_alive)? {
			futures::Async::Ready(packet) => new_packets_to_be_sent.push(packet),
			futures::Async::NotReady => (),
		}

		// -------
		// Publish
		// -------

		let mut publish_requests_waiting_to_be_sent = vec![];
		loop {
			match publish_request_recv.poll().expect("Receiver::poll cannot fail") {
				futures::Async::Ready(Some(publish_request)) =>
					publish_requests_waiting_to_be_sent.push(publish_request),

				futures::Async::Ready(None) |
				futures::Async::NotReady =>
					break,
			}
		}

		let (new_publish_packets, new_publications_received) = publish.poll(
			&mut packet,
			publish_requests_waiting_to_be_sent,
		)?;

		publications_received.extend(new_publications_received);
		new_packets_to_be_sent.extend(new_publish_packets);

		// -------------
		// Subscriptions
		// -------------

		let mut subscription_updates_waiting_to_be_sent = vec![];
		loop {
			match subscriptions_updated_recv.poll().expect("Receiver::poll cannot fail") {
				futures::Async::Ready(Some(subscription_to_update)) =>
					subscription_updates_waiting_to_be_sent.push(subscription_to_update),

				futures::Async::Ready(None) |
				futures::Async::NotReady =>
					break,
			}
		}

		new_packets_to_be_sent.extend(subscriptions.poll(
			&mut packet,
			subscription_updates_waiting_to_be_sent,
		)?);

		// ---

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

#[derive(Debug)]
struct PacketIdentifiers {
	in_use: std::collections::BTreeSet<crate::proto::PacketIdentifier>,
	previous: crate::proto::PacketIdentifier,
}

impl PacketIdentifiers {
	fn reserve(&mut self) -> crate::proto::PacketIdentifier {
		while self.in_use.contains(&self.previous) {
			self.previous =
				crate::proto::PacketIdentifier::new(match self.previous.get() {
					std::u16::MAX => 1,
					value => value + 1,
				})
				.expect("unreachable");
		}

		self.in_use.insert(self.previous);
		self.previous
	}

	fn discard(&mut self, packet_identifier: crate::proto::PacketIdentifier) {
		self.in_use.remove(&packet_identifier);
	}
}

impl Default for PacketIdentifiers {
	fn default() -> Self {
		PacketIdentifiers {
			in_use: Default::default(),
			previous: crate::proto::PacketIdentifier::new(1).expect("unreachable: 1 is not 0"),
		}
	}
}
