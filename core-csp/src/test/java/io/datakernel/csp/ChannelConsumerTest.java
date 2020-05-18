package io.datakernel.csp;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.exception.ExpectedException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.csp.ChannelConsumers.channelConsumerAsOutputStream;
import static io.datakernel.csp.ChannelConsumers.outputStreamAsChannelConsumer;
import static io.datakernel.eventloop.Eventloop.initWithEventloop;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.*;

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
		await(channelConsumer.acceptAll(
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
		await(channelConsumer.acceptAll(ByteBuf.empty(), ByteBuf.empty()));

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
		awaitException(channelConsumer.acceptAll(ByteBuf.empty(), ByteBuf.wrapForReading("Hello".getBytes())));
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

	@Test
	public void testOfAnotherEventloop() {
		Eventloop anotherEventloop = Eventloop.create();
		List<Integer> expectedList = asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		List<Integer> actualList = new ArrayList<>();
		ChannelConsumer<Integer> anotherEventloopConsumer = initWithEventloop(anotherEventloop, () -> ChannelConsumer.ofConsumer(actualList::add));
		ChannelConsumer<Integer> consumer = ChannelConsumer.ofAnotherEventloop(anotherEventloop, anotherEventloopConsumer);

		startAnotherEventloop(anotherEventloop);
		await(consumer.acceptAll(expectedList));
		stopAnotherEventloop(anotherEventloop);

		assertEquals(expectedList, actualList);
	}

	@Test
	public void testOfAnotherEventloopException() {
		Eventloop anotherEventloop = Eventloop.create();
		ExpectedException expectedException = new ExpectedException();
		List<Integer> list = new ArrayList<>();
		ChannelConsumer<Integer> anotherEventloopConsumer = initWithEventloop(anotherEventloop, () -> ChannelConsumer.ofConsumer(list::add));
		ChannelConsumer<Integer> consumer = ChannelConsumer.ofAnotherEventloop(anotherEventloop, anotherEventloopConsumer);

		startAnotherEventloop(anotherEventloop);
		Throwable exception = awaitException(consumer.accept(1)
				.then(() -> consumer.accept(2))
				.whenComplete(() -> consumer.closeEx(expectedException))
				.then(() -> consumer.accept(3)));
		stopAnotherEventloop(anotherEventloop);

		assertSame(expectedException, exception);
		assertEquals(asList(1,2), list);
	}


	private void startAnotherEventloop(Eventloop anotherEventloop) {
		anotherEventloop.keepAlive(true);
		new Thread(anotherEventloop, "another").start();
	}

	private void stopAnotherEventloop(Eventloop anotherEventloop) {
		anotherEventloop.execute(() -> anotherEventloop.keepAlive(false));
	}

}
