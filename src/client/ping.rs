use futures::Future;

pub(super) enum State {
	BeginWaitingForNextPing,
	WaitingForNextPing(tokio::timer::Delay),
	Invalid,
}

impl State {
	pub(super) fn poll(
		&mut self,
		packet: &mut Option<crate::proto::Packet>,
		keep_alive: std::time::Duration,
	) -> futures::Poll<crate::proto::Packet, std::io::Error> {
		match packet.take() {
			Some(crate::proto::Packet::PingResp) => *self = State::BeginWaitingForNextPing, // Reset ping timer
			other => *packet = other,
		}

		let mut current_state = std::mem::replace(self, State::Invalid);

		log::trace!("    {:?}", current_state);

		loop {
			let (next_state, result) = match current_state {
				State::BeginWaitingForNextPing => {
					let ping_timer_deadline = std::time::Instant::now() + keep_alive / 2;
					let ping_timer = tokio::timer::Delay::new(ping_timer_deadline);
					(State::WaitingForNextPing(ping_timer), None)
				},

				State::WaitingForNextPing(mut ping_timer) => match ping_timer.poll().map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))? {
					futures::Async::Ready(()) => {
						let packet = crate::proto::Packet::PingReq;
						(State::BeginWaitingForNextPing, Some(Ok(futures::Async::Ready(packet))))
					},

					futures::Async::NotReady =>
						(State::WaitingForNextPing(ping_timer), Some(Ok(futures::Async::NotReady))),
				},

				State::Invalid =>
					(State::Invalid, Some(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "polled in invalid state")))),
			};
			current_state = next_state;

			log::trace!("--> {:?}", current_state);

			if let Some(result) = result {
				*self = current_state;
				return result;
			}
		}
	}

	pub(super) fn reset(&mut self) {
		*self = State::BeginWaitingForNextPing;
	}
}

impl std::fmt::Debug for State {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			State::BeginWaitingForNextPing => f.write_str("BeginWaitingForNextPing"),
			State::WaitingForNextPing { .. } => f.write_str("WaitingForNextPing"),
			State::Invalid => f.write_str("Invalid"),
		}
	}
}
