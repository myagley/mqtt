///     rm -rf out/ && cargo afl build && cargo afl fuzz -i in -o out target/debug/mqtt-fuzz

use afl::fuzz;
use tokio::codec::Decoder;

fn main() {
	fuzz!(|data: &[u8]| {
		let mut codec: mqtt::proto::PacketCodec = Default::default();

		let mut bytes: bytes::BytesMut = data.into();

		let _ = codec.decode(&mut bytes);
	})
}
