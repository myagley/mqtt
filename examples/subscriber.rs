use futures::{ Future, Stream };

fn main() {
	env_logger::Builder::from_env("MQTT_LOG").init();

	let mut runtime = tokio::runtime::Runtime::new().expect("couldn't initialize tokio runtime");

	let client =
		mqtt::Client::new(
			Some("example-subscriber".to_string()),
			None,
			None,
			|| tokio::net::TcpStream::connect(&std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 1884)),
			std::time::Duration::from_secs(30),
			std::time::Duration::from_secs(5),
			10,
			10,
		);

	let mut update_subscription_handle = client.update_subscription_handle();
	runtime.spawn(
		update_subscription_handle
		.subscribe(mqtt::proto::SubscribeTo {
			topic_filter: "foo".to_string(),
			qos: mqtt::proto::QoS::AtLeastOnce,
		})
		.map_err(|err| panic!("couldn't update subscription: {}", err)));

	let f = client.for_each(|publications| {
		for publication in publications {
			log::info!(
				"Received publication: {:?} {:?} {:?}",
				publication.topic_name,
				std::str::from_utf8(&publication.payload).expect("test payloads are valid str"),
				publication.qos,
			);
		}
		Ok(())
	});

	runtime.block_on(f).expect("subscriber failed");
}
