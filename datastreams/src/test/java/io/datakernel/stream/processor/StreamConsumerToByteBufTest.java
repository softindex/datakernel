package io.datakernel.stream.processor;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static org.junit.Assert.assertArrayEquals;

public class StreamConsumerToByteBufTest {

	@Test
	public void testCommon() {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer.of(
				ByteBuf.wrapForReading(new byte[]{1}),
				ByteBuf.wrapForReading(new byte[]{2}),
				ByteBuf.wrapForReading(new byte[]{3, 4, 5, 6, 7}),
				ByteBuf.wrapForReading(new byte[]{8, 9, 10}),
				ByteBuf.wrapForReading(new byte[]{8, 9, 10})
		).streamTo(StreamConsumerToByteBuf.create())
				.getConsumerResult()
				.whenComplete(assertComplete(byteBuf ->
						assertArrayEquals(byteBuf.asArray(), new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 8, 9, 10})));

		eventloop.run();
	}

	@Test
	public void testEmpty() {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer.<ByteBuf>closing().streamTo(StreamConsumerToByteBuf.create())
				.getConsumerResult()
				.whenComplete(assertComplete(byteBuf -> assertArrayEquals(byteBuf.asArray(), new byte[0])));

		eventloop.run();
	}
}
