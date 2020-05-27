package io.datakernel.csp.binary;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static io.datakernel.csp.binary.ByteBufsDecoder.ofCrlfTerminatedBytes;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class BinaryChannelSupplierTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testParseStream() {
		ByteBuf buf = await(BinaryChannelSupplier.of(ChannelSupplier.of(wrapUtf8("Hello\r\n World\r\n")))
				.parseStream(ofCrlfTerminatedBytes())
				.toCollector(ByteBufQueue.collector()));
		assertEquals("Hello World", buf.asString(UTF_8));
	}

	@Test
	public void testParseStreamLessData() {
		Exception exception = awaitException(BinaryChannelSupplier.of(ChannelSupplier.of(wrapUtf8("Hello\r\n Wo")))
				.parseStream(ofCrlfTerminatedBytes())
				.toCollector(ByteBufQueue.collector()));
		assertSame(UNEXPECTED_END_OF_STREAM_EXCEPTION, exception);
	}
}
