use futures::Future;

pub(super) enum State {
	BeginWaitingForNextPing,
	WaitingForNextPing(tokio::timer::Delay),
}

impl State {
	pub(super) fn poll(
		&mut self,
		packet: &mut Option<crate::proto::Packet>,
		keep_alive: std::time::Duration,
	) -> futures::Poll<crate::proto::Packet, super::Error> {
		match packet.take() {
			Some(crate::proto::Packet::PingResp) => *self = State::BeginWaitingForNextPing, // Reset ping timer
			other => *packet = other,
		}

		loop {
			log::trace!("    {:?}", self);

			match self {
				State::BeginWaitingForNextPing => {
					let ping_timer_deadline = std::time::Instant::now() + keep_alive / 2;
					let ping_timer = tokio::timer::Delay::new(ping_timer_deadline);
					*self = State::WaitingForNextPing(ping_timer);
				},

				State::WaitingForNextPing(ping_timer) => match ping_timer.poll().map_err(super::Error::PingTimer)? {
					futures::Async::Ready(()) => {
						let packet = crate::proto::Packet::PingReq;
						*self = State::BeginWaitingForNextPing;
						return Ok(futures::Async::Ready(packet));
					},

					futures::Async::NotReady =>
						return Ok(futures::Async::NotReady),
				},
			}
		}
	}

	pub(super) fn new_connection(&mut self) {
		*self = State::BeginWaitingForNextPing;
	}
}

impl std::fmt::Debug for State {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			State::BeginWaitingForNextPing => f.write_str("BeginWaitingForNextPing"),
			State::WaitingForNextPing { .. } => f.write_str("WaitingForNextPing"),
		}
	}
}
