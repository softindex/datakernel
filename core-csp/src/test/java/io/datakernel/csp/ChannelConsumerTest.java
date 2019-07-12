package io.datakernel.csp;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.csp.ChannelConsumers.channelConsumerAsOutputStream;
import static io.datakernel.csp.ChannelConsumers.outputStreamAsChannelConsumer;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChannelConsumerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testToOutputStream() {
		ByteBuf buf = ByteBufPool.allocate(100);
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int i) {
				buf.writeByte((byte) i);
			}
		};

		ChannelConsumer<ByteBuf> channelConsumer = outputStreamAsChannelConsumer(newSingleThreadExecutor(), outputStream);
		await(channelConsumer.accept(
				ByteBuf.wrapForReading("Hello".getBytes()),
				ByteBuf.wrapForReading("World".getBytes())));

		assertEquals(buf.asString(Charset.defaultCharset()), "HelloWorld");
	}

	@Test
	public void testToOutputStreamEmpty() {
		ByteBuf buf = ByteBufPool.allocate(100);
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int i) {
				buf.writeByte((byte) i);
			}
		};

		ChannelConsumer<ByteBuf> channelConsumer = outputStreamAsChannelConsumer(newSingleThreadExecutor(), outputStream);
		await(channelConsumer.accept(ByteBuf.empty(), ByteBuf.empty()));

		assertEquals(buf.asString(Charset.defaultCharset()), "");
	}

	@Test
	public void testToOutputStreamException() {
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int i) throws IOException {
				throw new IOException("Some exception");
			}
		};

		ChannelConsumer<ByteBuf> channelConsumer = outputStreamAsChannelConsumer(newSingleThreadExecutor(), outputStream);
		awaitException(channelConsumer.accept(ByteBuf.empty(), ByteBuf.wrapForReading("Hello".getBytes())));
	}

	@Test
	public void testAsOutputStream() {
		int expectedSize = 1000;

		ByteBuf result = ByteBuf.wrapForWriting(new byte[expectedSize]);

		ChannelConsumer<ByteBuf> channelConsumer = ChannelConsumer.of(
				buf -> {
					result.put(buf);
					buf.recycle();
					return Promise.complete();
				});

		Eventloop currentEventloop = Eventloop.getCurrentEventloop();
		await(Promise.ofBlockingCallable(newSingleThreadExecutor(),
				() -> {
					OutputStream outputStream = channelConsumerAsOutputStream(currentEventloop, channelConsumer);
					for (int i = 0; i < expectedSize; i++) {
						outputStream.write(i);
					}
					outputStream.flush();
					return null;
				}));

		for (int i = 0; i < expectedSize; i++) {
			assertEquals((byte) i, result.array()[i]);
		}
	}

	@Test
	public void testAsOutputStreamEmpty() {
		int expectedSize = 0;

		ChannelConsumer<ByteBuf> channelConsumer = ChannelConsumer.of(value -> {
			assertEquals(expectedSize, value.readRemaining());
			value.recycle();
			return Promise.complete();
		});

		Eventloop currentEventloop = Eventloop.getCurrentEventloop();
		await(Promise.ofBlockingCallable(newSingleThreadExecutor(), () -> {
			try {
				OutputStream outputStream = channelConsumerAsOutputStream(currentEventloop, channelConsumer);
				outputStream.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}));
	}

	@Test
	public void testAsOutputStreamException() {
		ChannelConsumer<ByteBuf> channelConsumer = ChannelConsumer.of(value -> {
			value.recycle();
			return Promise.ofException(new RuntimeException());
		});

		Eventloop currentEventloop = Eventloop.getCurrentEventloop();
		await(Promise.ofBlockingCallable(newSingleThreadExecutor(), () -> {
			try {
				OutputStream outputStream = channelConsumerAsOutputStream(currentEventloop, channelConsumer);
				outputStream.flush();
			} catch (Exception e) {
				assertTrue(e instanceof RuntimeException);
			}
			return null;
		}));
	}
}
