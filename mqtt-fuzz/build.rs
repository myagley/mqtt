/// Writes the inputs for the fuzzer

use std::io::Write;

use tokio::codec::Encoder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let in_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("in");
	if in_dir.exists() {
		std::fs::remove_dir_all(&in_dir)?;
	}
	std::fs::create_dir(&in_dir)?;

	let packets = vec![
		("connack", mqtt::proto::Packet::ConnAck(mqtt::proto::ConnAck {
			session_present: true,
			return_code: mqtt::proto::ConnectReturnCode::Accepted,
		})),

		("connect", mqtt::proto::Packet::Connect(mqtt::proto::Connect {
			username: Some("username".to_string()),
			password: Some("password".to_string()),
			will: Some(mqtt::proto::Publication {
				topic_name: "will-topic".to_string(),
				qos: mqtt::proto::QoS::ExactlyOnce,
				retain: true,
				payload: b"\x00\x01\x02\xFF\xFE\xFD"[..].into(),
			}),
			client_id: mqtt::proto::ClientId::IdWithExistingSession("id".to_string()),
			keep_alive: std::time::Duration::from_secs(5),
		})),

		("disconnect", mqtt::proto::Packet::Disconnect(mqtt::proto::Disconnect)),

		("pingreq", mqtt::proto::Packet::PingReq(mqtt::proto::PingReq)),

		("pingresp", mqtt::proto::Packet::PingResp(mqtt::proto::PingResp)),

		("puback", mqtt::proto::Packet::PubAck(mqtt::proto::PubAck {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
		})),

		("pubcomp", mqtt::proto::Packet::PubComp(mqtt::proto::PubComp {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
		})),

		("publish", mqtt::proto::Packet::Publish(mqtt::proto::Publish {
			packet_identifier_dup_qos: mqtt::proto::PacketIdentifierDupQoS::ExactlyOnce(mqtt::proto::PacketIdentifier::new(5).unwrap(), true),
			retain: true,
			topic_name: "publish-topic".to_string(),
			payload: b"\x00\x01\x02\xFF\xFE\xFD"[..].into(),
		})),

		("pubrec", mqtt::proto::Packet::PubRec(mqtt::proto::PubRec {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
		})),

		("pubrel", mqtt::proto::Packet::PubRel(mqtt::proto::PubRel {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
		})),

		("suback", mqtt::proto::Packet::SubAck(mqtt::proto::SubAck {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
			qos: vec![
				mqtt::proto::SubAckQos::Success(mqtt::proto::QoS::ExactlyOnce),
				mqtt::proto::SubAckQos::Failure,
			],
		})),

		("subscribe", mqtt::proto::Packet::Subscribe(mqtt::proto::Subscribe {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
			subscribe_to: vec![
				mqtt::proto::SubscribeTo {
					topic_filter: "subscribe-topic".to_string(),
					qos: mqtt::proto::QoS::ExactlyOnce,
				},
			],
		})),

		("unsuback", mqtt::proto::Packet::UnsubAck(mqtt::proto::UnsubAck {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
		})),

		("unsubscribe", mqtt::proto::Packet::Unsubscribe(mqtt::proto::Unsubscribe {
			packet_identifier: mqtt::proto::PacketIdentifier::new(5).unwrap(),
			unsubscribe_from: vec![
				"unsubscribe-topic".to_string(),
			],
		})),
	];

	for (filename, packet) in packets {
		let file = std::fs::OpenOptions::new().create(true).write(true).open(in_dir.join(filename))?;
		let mut file = std::io::BufWriter::new(file);

		let mut codec: mqtt::proto::PacketCodec = Default::default();

		let mut bytes = bytes::BytesMut::new();

		codec.encode(packet, &mut bytes)?;

		file.write_all(&bytes)?;

		file.flush()?;
	}

	Ok(())
}
