import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsDecoder;
import io.datakernel.eventloop.Eventloop;

import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

//[START EXAMPLE]
public final class ByteBufsDecoderExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		List<ByteBuf> letters = asList(wrapAscii("H"), wrapAscii("e"), wrapAscii("l"), wrapAscii("l"), wrapAscii("o"));
		ByteBufsDecoder<String> decoder = bufs -> {
			if (!bufs.hasRemainingBytes(5)) {
				System.out.println("Not enough bytes to decode message");
				return null;
			}
			return bufs.takeExactSize(5).asString(UTF_8);
		};

		BinaryChannelSupplier.of(ChannelSupplier.ofIterable(letters)).parse(decoder)
				.whenResult(x -> System.out.println(x));

		eventloop.run();
	}
}
//[END EXAMPLE]
