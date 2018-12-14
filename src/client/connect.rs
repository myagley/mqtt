use futures::{ Future, Sink, Stream };

#[derive(Debug)]
pub(super) struct Connect<IoS> where IoS: super::IoSource {
	io_source: IoS,
	max_back_off: std::time::Duration,
	current_back_off: std::time::Duration,
	state: State<IoS>,
}

enum State<IoS> where IoS: super::IoSource {
	BeginBackOff,
	EndBackOff(tokio::timer::Delay),
	BeginConnecting,
	WaitingForIoToConnect(<IoS as super::IoSource>::Future),
	BeginSendingConnect(crate::logging_framed::LoggingFramed<<IoS as super::IoSource>::Io>),
	EndSendingConnect(crate::logging_framed::LoggingFramed<<IoS as super::IoSource>::Io>),
	WaitingForConnAck(crate::logging_framed::LoggingFramed<<IoS as super::IoSource>::Io>),
	Connected {
		framed: crate::logging_framed::LoggingFramed<<IoS as super::IoSource>::Io>,
		new_connection: bool,
		reset_session: bool,
	},
	Invalid,
}

impl<IoS> std::fmt::Debug for State<IoS> where IoS: super::IoSource {
	#[allow(
		clippy::unneeded_field_pattern, // Clippy wants wildcard pattern for Connected,
		                                // which would silently allow fields to be added to the variant without adding them here
	)]
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			State::BeginBackOff => f.write_str("BeginBackOff"),
			State::EndBackOff(_) => f.write_str("EndBackOff"),
			State::BeginConnecting => f.write_str("BeginConnecting"),
			State::WaitingForIoToConnect(_) => f.write_str("WaitingForIoToConnect"),
			State::BeginSendingConnect(_) => f.write_str("BeginSendingConnect"),
			State::EndSendingConnect(_) => f.write_str("EndSendingConnect"),
			State::WaitingForConnAck(_) => f.write_str("WaitingForConnAck"),
			State::Connected { framed: _, new_connection, reset_session } =>
				f.debug_struct("Connected")
				.field("new_connection", new_connection)
				.field("reset_session", reset_session)
				.finish(),
			State::Invalid => f.write_str("Invalid"),
		}
	}
}

impl<IoS> Connect<IoS> where IoS: super::IoSource {
	pub(super) fn new(io_source: IoS, max_back_off: std::time::Duration) -> Self {
		Connect {
			io_source,
			max_back_off,
			current_back_off: std::time::Duration::from_secs(0),
			state: State::BeginConnecting,
		}
	}

	pub(super) fn reconnect(&mut self) {
		self.state = State::BeginBackOff;
	}
}

impl<IoS> Connect<IoS> where IoS: super::IoSource, <<IoS as super::IoSource>::Future as Future>::Error: std::fmt::Display {
	pub(super) fn poll<'a>(
		&'a mut self,
		username: Option<&str>,
		password: Option<&str>,
		client_id: &mut crate::proto::ClientId,
		keep_alive: std::time::Duration,
	) -> futures::Poll<Connected<'a, IoS>, ()> {
		let mut current_state = std::mem::replace(&mut self.state, State::Invalid);

		log::trace!("    {:?}", current_state);

		loop {
			let (next_state, result) = match current_state {
				State::BeginBackOff => match self.current_back_off {
					back_off if back_off.as_secs() == 0 => {
						self.current_back_off = std::time::Duration::from_secs(1);
						(State::BeginConnecting, None)
					},

					back_off => {
						log::debug!("Backing off for {:?}", back_off);
						let back_off_deadline = std::time::Instant::now() + back_off;
						self.current_back_off = std::cmp::min(self.max_back_off, self.current_back_off * 2);
						(State::EndBackOff(tokio::timer::Delay::new(back_off_deadline)), None)
					},
				},

				State::EndBackOff(mut back_off_timer) => match back_off_timer.poll().expect("TODO: handle Delay error") {
					futures::Async::Ready(()) => (State::BeginConnecting, None),
					futures::Async::NotReady => (State::EndBackOff(back_off_timer), Some(futures::Async::NotReady)),
				},

				State::BeginConnecting => {
					let io = self.io_source.connect();
					(State::WaitingForIoToConnect(io), None)
				},

				State::WaitingForIoToConnect(mut io) => match io.poll() {
					Ok(futures::Async::Ready(io)) => {
						let framed = crate::logging_framed::LoggingFramed::new(io);
						(State::BeginSendingConnect(framed), None)
					},

					Ok(futures::Async::NotReady) => (State::WaitingForIoToConnect(io), Some(futures::Async::NotReady)),

					Err(err) => {
						log::warn!("could not connect to server: {}", err);
						(State::BeginBackOff, None)
					},
				},

				State::BeginSendingConnect(mut framed) => {
					let packet = crate::proto::Packet::Connect {
						username: username.map(ToOwned::to_owned),
						password: password.map(ToOwned::to_owned),
						client_id: client_id.clone(),
						keep_alive,
					};

					match framed.start_send(packet) {
						Ok(futures::AsyncSink::Ready) => (State::EndSendingConnect(framed), None),
						Ok(futures::AsyncSink::NotReady(_)) => (State::BeginSendingConnect(framed), Some(futures::Async::NotReady)),
						Err(err) => {
							log::warn!("could not connect to server: {}", err);
							(State::BeginBackOff, None)
						},
					}
				},

				State::EndSendingConnect(mut framed) => match framed.poll_complete() {
					Ok(futures::Async::Ready(())) => (State::WaitingForConnAck(framed), None),
					Ok(futures::Async::NotReady) => (State::EndSendingConnect(framed), Some(futures::Async::NotReady)),
					Err(err) => {
						log::warn!("could not connect to server: {}", err);
						(State::BeginBackOff, None)
					},
				},

				State::WaitingForConnAck(mut framed) => match framed.poll() {
					Ok(futures::Async::Ready(Some(packet))) => match packet {
						crate::proto::Packet::ConnAck { session_present, return_code: crate::proto::ConnectReturnCode::Accepted } => {
							self.current_back_off = std::time::Duration::from_secs(0);

							let reset_session = match client_id {
								crate::proto::ClientId::ServerGenerated => true,
								crate::proto::ClientId::IdWithCleanSession(id) => {
									*client_id = crate::proto::ClientId::IdWithExistingSession(std::mem::replace(id, Default::default()));
									true
								},
								crate::proto::ClientId::IdWithExistingSession(id) => {
									*client_id = crate::proto::ClientId::IdWithExistingSession(std::mem::replace(id, Default::default()));
									!session_present
								},
							};

							(State::Connected { framed, new_connection: true, reset_session }, None)
						},

						crate::proto::Packet::ConnAck { return_code: crate::proto::ConnectReturnCode::Refused(return_code), .. } => {
							log::warn!("could not connect to server: connection refused: {:?}", return_code);
							(State::BeginBackOff, None)
						},

						packet => {
							log::warn!("could not connect to server: expected to receive ConnAck but received {:?}", packet);
							(State::BeginBackOff, None)
						},
					},

					Ok(futures::Async::Ready(None)) => {
						log::warn!("could not connect to server: connection closed by server");
						(State::BeginBackOff, None)
					},

					Ok(futures::Async::NotReady) => (State::WaitingForConnAck(framed), Some(futures::Async::NotReady)),

					Err(err) => {
						log::warn!("could not connect to server: {}", err);
						(State::BeginBackOff, None)
					},
				},

				State::Connected { framed, new_connection, reset_session } =>
					(State::Connected { framed, new_connection: false, reset_session: false }, Some(futures::Async::Ready((new_connection, reset_session)))),

				State::Invalid => unreachable!(),
			};
			current_state = next_state;

			log::trace!("--> {:?}", current_state);

			match result {
				Some(futures::Async::Ready((new_connection, reset_session))) => {
					self.state = current_state;

					match &mut self.state {
						State::Connected { framed, .. } => break Ok(futures::Async::Ready(Connected {
							framed,
							new_connection,
							reset_session,
						})),
						_ => unreachable!(),
					}
				},

				Some(futures::Async::NotReady) => {
					self.state = current_state;

					break Ok(futures::Async::NotReady);
				},

				None => (),
			}
		}
	}
}

pub(super) struct Connected<'a, IoS> where IoS: super::IoSource {
	pub(super) framed: &'a mut crate::logging_framed::LoggingFramed<<IoS as super::IoSource>::Io>,
	pub(super) new_connection: bool,
	pub(super) reset_session: bool,
}
