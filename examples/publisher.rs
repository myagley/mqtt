use futures::{ Future, Stream };

fn main() {
	env_logger::Builder::from_env("MQTT_LOG").init();

	let mut runtime = tokio::runtime::Runtime::new().expect("couldn't initialize tokio runtime");
	let executor = runtime.executor();

	let client =
		mqtt::Client::new(
			Some("example-publisher".to_string()),
			None,
			None,
			|| tokio::net::TcpStream::connect(&std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 1884)),
			std::time::Duration::from_secs(30),
			std::time::Duration::from_secs(5),
			10,
			10,
		);

	let mut publish_handle = client.publish_handle();
	let publish_loop =
		tokio::timer::Interval::new(std::time::Instant::now(), std::time::Duration::from_millis(1000))
		.then(move |result| {
			let _ = result.expect("timer failed");

			let topic_name = "foo".to_string();

			log::debug!("Publishing to {} ...", topic_name);

			publish_handle
				.publish(mqtt::Publication {
					topic_name: topic_name.clone(),
					qos: mqtt::proto::QoS::AtLeastOnce,
					retain: false,
					payload: b"hello, world".to_vec(),
				})
				.expect("couldn't publish")
				.then(|result| {
					let () = result.expect("couldn't complete publish");
					Ok(topic_name)
				})
		})
		.for_each(|topic_name| {
			log::debug!("Published to {}", topic_name);
			Ok(())
		});
	executor.spawn(publish_loop);

	let f = client.for_each(|_| Ok(()));

	runtime.block_on(f).expect("subscriber failed");
}
