/// Parses the given file as an MQTT packet and prints it to stdout
///
/// Example:
///
///     cargo run --example decode -- /path/to/some/raw/mqtt/packet.bin

use std::io::Read;

use tokio::codec::Decoder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let filename = std::env::args_os().nth(1).ok_or("expected one argument set to the name of the file to decode")?;
	let file = std::fs::OpenOptions::new().read(true).open(filename)?;
	let mut file = std::io::BufReader::new(file);

	let mut codec: mqtt::proto::PacketCodec = Default::default();

	let mut bytes = vec![];
	file.read_to_end(&mut bytes)?;
	let mut bytes: bytes::BytesMut = bytes.into();

	let packet = codec.decode(&mut bytes)?.ok_or("incomplete packet")?;

	if !bytes.is_empty() {
		return Err("leftover bytes".into());
	}

	println!("{:#?}", packet);

	Ok(())
}
