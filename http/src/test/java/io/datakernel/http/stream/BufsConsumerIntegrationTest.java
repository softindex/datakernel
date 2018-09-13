package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.http.TestUtils.AssertingConsumer;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.test.TestUtils.assertComplete;
import static org.junit.Assert.assertTrue;

public class BufsConsumerIntegrationTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();
	public final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	public final AssertingConsumer consumer = new AssertingConsumer();
	public final ByteBufQueue queue = new ByteBufQueue();
	public final Random random = new Random();
	public BufsConsumer decoder;
	public BufsConsumer encoder;

	@Before
	public void setUp() {
		queue.recycle();
		consumer.reset();
	}

	@Test
	public void testEncodeDecodeSingleBuf() {
		decoder = new BufsConsumerChunkedDecoder(consumer);
		encoder = new BufsConsumerChunkedEncoder(decoder);
		testSingleBuf();
	}

	@Test
	public void testEncodeDecodeMultipleBufs() {
		decoder = new BufsConsumerChunkedDecoder(consumer);
		encoder = new BufsConsumerChunkedEncoder(decoder);
		testMultipleBufs();
	}

	@Test
	public void testGzipGunzipSingleBuf() {
		decoder = new BufsConsumerGzipInflater(consumer);
		encoder = new BufsConsumerGzipDeflater(decoder);
		testSingleBuf();
	}

	@Test
	public void testGzipGunzipMultipleBufs() {
		decoder = new BufsConsumerGzipInflater(consumer);
		encoder = new BufsConsumerGzipDeflater(decoder);
		testMultipleBufs();
	}

	private void testSingleBuf() {
		byte[] data = new byte[1000];
		random.nextBytes(data);
		consumer.setExpectedByteArray(data);
		ByteBuf buf = ByteBufPool.allocate(data.length);
		buf.put(data);
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf))
				.thenCompose($ -> encoder.push(queue, true))
				.whenComplete(assertComplete(Assert::assertTrue));
		future.complete(null);

		eventloop.run();
	}

	private void testMultipleBufs() {
		byte[] data = new byte[100_000];
		boolean[] wasExecuted = {false};
		random.nextBytes(data);
		ByteBuf toBeSplitted = ByteBufPool.allocate(data.length);
		ByteBuf expected = ByteBufPool.allocate(data.length);
		toBeSplitted.put(data);
		expected.put(data);
		consumer.setExpectedBuf(expected);
		while (toBeSplitted.isRecycleNeeded() && toBeSplitted.readRemaining() != 0) {
			int part = Math.min(random.nextInt(100) + 100, toBeSplitted.readRemaining());
			ByteBuf slice = toBeSplitted.slice(part);
			toBeSplitted.moveReadPosition(part);
			queue.add(slice);
			boolean lastChunk = toBeSplitted.readRemaining() == 0;
			encoder.push(queue, lastChunk)
					.whenComplete(assertComplete(r -> {
						if (lastChunk) {
							wasExecuted[0] = true;
							assertTrue(r);
							toBeSplitted.recycle();
						}
					}));
		}
		assertTrue(wasExecuted[0]);
	}
}
