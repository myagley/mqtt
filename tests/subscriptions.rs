mod common;

#[test]
fn server_generated_id_must_always_resubscribe() {
	let mut runtime = tokio::runtime::current_thread::Runtime::new().expect("couldn't initialize tokio runtime");

	let (io_source, done) = common::IoSource::new(vec![
		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::ServerGenerated,
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: false,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				subscribe_to: vec![
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtMostOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::ExactlyOnce),
				],
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],

		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::ServerGenerated,
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: false,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
				packet_identifier: mqtt::proto::PacketIdentifier::new(2).unwrap(),
				subscribe_to: vec![
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(2).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtMostOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::ExactlyOnce),
				],
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],
	]);

	let mut client =
		mqtt::Client::new(
			None,
			None,
			None,
			io_source,
			std::time::Duration::from_secs(0),
			std::time::Duration::from_secs(4),
		);
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }).unwrap();
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }).unwrap();
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }).unwrap();

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }),
		]),
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }),
		]),
		mqtt::Event::NewConnection { reset_session: false },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }),
		]),
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
}

#[test]
fn client_id_should_not_resubscribe_when_session_is_present() {
	let mut runtime = tokio::runtime::current_thread::Runtime::new().expect("couldn't initialize tokio runtime");

	let (io_source, done) = common::IoSource::new(vec![
		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithCleanSession("idle_client_id".to_string()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: false,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				subscribe_to: vec![
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtMostOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::ExactlyOnce),
				],
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],

		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithExistingSession("idle_client_id".to_string()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				// The clean session bit also determines if the *current* session should be persisted.
				// So when the previous session requested a clean session, the server would not persist *that* session either.
				// So this second session will still have `session_present == false`
				session_present: false,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
				packet_identifier: mqtt::proto::PacketIdentifier::new(2).unwrap(),
				subscribe_to: vec![
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(2).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtMostOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::ExactlyOnce),
				],
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],

		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithExistingSession("idle_client_id".to_string()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: true,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],
	]);

	let mut client =
		mqtt::Client::new(
			Some("idle_client_id".to_string()),
			None,
			None,
			io_source,
			std::time::Duration::from_secs(0),
			std::time::Duration::from_secs(4),
		);
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }).unwrap();
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }).unwrap();
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }).unwrap();

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }),
		]),
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }),
		]),
		mqtt::Event::NewConnection { reset_session: false },
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
}

#[test]
fn should_combine_pending_subscription_updates() {
	let mut runtime = tokio::runtime::current_thread::Runtime::new().expect("couldn't initialize tokio runtime");

	let (io_source, done) = common::IoSource::new(vec![
		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::ServerGenerated,
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: false,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				subscribe_to: vec![
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtLeastOnce },
					mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::ExactlyOnce),
				],
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],
	]);

	let mut client =
		mqtt::Client::new(
			None,
			None,
			None,
			io_source,
			std::time::Duration::from_secs(0),
			std::time::Duration::from_secs(4),
		);
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtMostOnce }).unwrap();
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic2".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }).unwrap();
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }).unwrap();
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }).unwrap();
	client.unsubscribe("topic2".to_string()).unwrap();

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_string(), qos: mqtt::proto::QoS::AtLeastOnce }),
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic3".to_string(), qos: mqtt::proto::QoS::ExactlyOnce }),
		]),
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
}

#[test]
fn should_reject_invalid_subscriptions() {
	let (io_source, _) = common::IoSource::new(vec![]);

	let mut client =
		mqtt::Client::new(
			None,
			None,
			None,
			io_source,
			std::time::Duration::from_secs(0),
			std::time::Duration::from_secs(4),
		);

	let too_large_topic_filter = "a".repeat(usize::from(u16::max_value()) + 1);

	match client.subscribe(mqtt::proto::SubscribeTo { topic_filter: too_large_topic_filter.clone(), qos: mqtt::proto::QoS::AtMostOnce }) {
		Err(mqtt::UpdateSubscriptionError::EncodePacket(_, mqtt::proto::EncodeError::StringTooLarge(_))) => (),
		result => panic!("expected client.subscribe() to fail with EncodePacket(StringTooLarge) but it returned {:?}", result),
	}
	match client.unsubscribe(too_large_topic_filter.clone()) {
		Err(mqtt::UpdateSubscriptionError::EncodePacket(_, mqtt::proto::EncodeError::StringTooLarge(_))) => (),
		result => panic!("expected client.unsubscribe() to fail with EncodePacket(StringTooLarge) but it returned {:?}", result),
	}
}
