mod common;

#[test]
fn server_publishes_at_most_once() {
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
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtMostOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtMostOnce),
				],
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::Publish(mqtt::proto::Publish {
				packet_identifier_dup_qos: mqtt::proto::PacketIdentifierDupQoS::AtMostOnce,
				retain: false,
				topic_name: "topic1".to_owned(),
				payload: [0x01, 0x02, 0x03][..].into(),
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
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtMostOnce }).unwrap();

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtMostOnce }),
		]),
		mqtt::Event::Publication(mqtt::ReceivedPublication {
			topic_name: "topic1".to_owned(),
			dup: false,
			qos: mqtt::proto::QoS::AtMostOnce,
			retain: false,
			payload: [0x01, 0x02, 0x03][..].into(),
		}),
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
}

#[test]
fn server_publishes_at_least_once() {
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
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
				],
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::Publish(mqtt::proto::Publish {
				packet_identifier_dup_qos: mqtt::proto::PacketIdentifierDupQoS::AtLeastOnce(mqtt::proto::PacketIdentifier::new(2).unwrap(), false),
				retain: false,
				topic_name: "topic1".to_owned(),
				payload: [0x01, 0x02, 0x03][..].into(),
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PubAck(mqtt::proto::PubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(2).unwrap(),
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
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce }).unwrap();

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce }),
		]),
		mqtt::Event::Publication(mqtt::ReceivedPublication {
			topic_name: "topic1".to_owned(),
			dup: false,
			qos: mqtt::proto::QoS::AtLeastOnce,
			retain: false,
			payload: [0x01, 0x02, 0x03][..].into(),
		}),
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
}

#[test]
fn server_publishes_at_least_once_with_reconnect_before_publish() {
	let mut runtime = tokio::runtime::current_thread::Runtime::new().expect("couldn't initialize tokio runtime");

	let (io_source, done) = common::IoSource::new(vec![
		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithCleanSession("client_id".to_owned()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: false,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),
		],

		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithExistingSession("client_id".to_owned()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: true,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				subscribe_to: vec![
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
				],
			})),
		],

		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithExistingSession("client_id".to_owned()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: true,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::Publish(mqtt::proto::Publish {
				packet_identifier_dup_qos: mqtt::proto::PacketIdentifierDupQoS::AtLeastOnce(mqtt::proto::PacketIdentifier::new(1).unwrap(), true),
				retain: false,
				topic_name: "topic1".to_owned(),
				payload: [0x01, 0x02, 0x03][..].into(),
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PubAck(mqtt::proto::PubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],
	]);

	let mut client =
		mqtt::Client::new(
			Some("client_id".to_owned()),
			None,
			None,
			io_source,
			std::time::Duration::from_secs(0),
			std::time::Duration::from_secs(4),
		);
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce }).unwrap();

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::NewConnection { reset_session: false },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce }),
		]),
		mqtt::Event::NewConnection { reset_session: false },
		mqtt::Event::Publication(mqtt::ReceivedPublication {
			topic_name: "topic1".to_owned(),
			dup: true,
			qos: mqtt::proto::QoS::AtLeastOnce,
			retain: false,
			payload: [0x01, 0x02, 0x03][..].into(),
		}),
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
}

#[test]
fn server_publishes_at_least_once_with_reconnect_before_ack() {
	let mut runtime = tokio::runtime::current_thread::Runtime::new().expect("couldn't initialize tokio runtime");

	let (io_source, done) = common::IoSource::new(vec![
		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithCleanSession("client_id".to_owned()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: false,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),
		],

		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithExistingSession("client_id".to_owned()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: true,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				subscribe_to: vec![
					mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce },
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
				qos: vec![
					mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::AtLeastOnce),
				],
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::Publish(mqtt::proto::Publish {
				packet_identifier_dup_qos: mqtt::proto::PacketIdentifierDupQoS::AtLeastOnce(mqtt::proto::PacketIdentifier::new(1).unwrap(), false),
				retain: false,
				topic_name: "topic1".to_owned(),
				payload: [0x01, 0x02, 0x03][..].into(),
			})),
		],

		vec![
			common::TestConnectionStep::Receives(mqtt::proto::Packet::Connect(mqtt::proto::Connect {
				username: None,
				password: None,
				will: None,
				client_id: mqtt::proto::ClientId::IdWithExistingSession("client_id".to_owned()),
				keep_alive: std::time::Duration::from_secs(4),
			})),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
				session_present: true,
				return_code: mqtt::proto::ConnectReturnCode::Accepted,
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::Publish(mqtt::proto::Publish {
				packet_identifier_dup_qos: mqtt::proto::PacketIdentifierDupQoS::AtLeastOnce(mqtt::proto::PacketIdentifier::new(1).unwrap(), true),
				retain: false,
				topic_name: "topic1".to_owned(),
				payload: [0x01, 0x02, 0x03][..].into(),
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PubAck(mqtt::proto::PubAck {
				packet_identifier: mqtt::proto::PacketIdentifier::new(1).unwrap(),
			})),

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),
		],
	]);

	let mut client =
		mqtt::Client::new(
			Some("client_id".to_owned()),
			None,
			None,
			io_source,
			std::time::Duration::from_secs(0),
			std::time::Duration::from_secs(4),
		);
	client.subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce }).unwrap();

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
		mqtt::Event::NewConnection { reset_session: false },
		mqtt::Event::SubscriptionUpdates(vec![
			mqtt::SubscriptionUpdateEvent::Subscribe(mqtt::proto::SubscribeTo { topic_filter: "topic1".to_owned(), qos: mqtt::proto::QoS::AtLeastOnce }),
		]),
		mqtt::Event::Publication(mqtt::ReceivedPublication {
			topic_name: "topic1".to_owned(),
			dup: false,
			qos: mqtt::proto::QoS::AtLeastOnce,
			retain: false,
			payload: [0x01, 0x02, 0x03][..].into(),
		}),
		mqtt::Event::NewConnection { reset_session: false },
		mqtt::Event::Publication(mqtt::ReceivedPublication {
			topic_name: "topic1".to_owned(),
			dup: true,
			qos: mqtt::proto::QoS::AtLeastOnce,
			retain: false,
			payload: [0x01, 0x02, 0x03][..].into(),
		}),
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
}

#[test]
fn should_reject_invalid_publications() {
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

			common::TestConnectionStep::Receives(mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

			common::TestConnectionStep::Sends(mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),

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

	let too_large_topic_name = "a".repeat(usize::from(u16::max_value()) + 1);

	let publish_future = client.publish(mqtt::proto::Publication {
		topic_name: too_large_topic_name,
		qos: mqtt::proto::QoS::AtMostOnce,
		retain: false,
		payload: Default::default(),
	});

	common::verify_client_events(&mut runtime, client, vec![
		mqtt::Event::NewConnection { reset_session: true },
	]);

	runtime.block_on(done).expect("connection broken while there were still steps remaining on the server");
	match runtime.block_on(publish_future) {
		Err(mqtt::PublishError::EncodePacket(_, mqtt::proto::EncodeError::StringTooLarge(_))) => (),
		result => panic!("expected client.publish() to fail with EncodePacket(StringTooLarge) but it returned {:?}", result),
	}
}
