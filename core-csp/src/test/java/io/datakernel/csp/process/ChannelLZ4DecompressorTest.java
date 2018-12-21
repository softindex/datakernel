package io.datakernel.csp.process;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import net.jpountz.lz4.LZ4Factory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.datakernel.csp.binary.BinaryChannelSupplier.UNEXPECTED_DATA_EXCEPTION;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;

@RunWith(DatakernelRunner.class)
public class ChannelLZ4DecompressorTest {

	@Test
	public void testTruncatedData() {
		ChannelLZ4Compressor compressor = ChannelLZ4Compressor.create(LZ4Factory.fastestInstance().fastCompressor());
		ChannelLZ4Decompressor decompressor = ChannelLZ4Decompressor.create();
		ByteBufQueue queue = new ByteBufQueue();

		ChannelSupplier.of(ByteBufStrings.wrapAscii("TestData")).transformWith(compressor)
				.streamTo(ChannelConsumer.ofConsumer(queue::add));

		// add trailing 0 - bytes
		queue.add(ByteBuf.wrapForReading(new byte[10]));

		ChannelSupplier.of(queue.takeRemaining())
				.transformWith(decompressor)
				.streamTo(ChannelConsumer.ofConsumer(data -> System.out.println(data.asString(UTF_8))))
				.whenComplete(assertFailure(e -> Assert.assertSame(UNEXPECTED_DATA_EXCEPTION, e)));
	}
}
