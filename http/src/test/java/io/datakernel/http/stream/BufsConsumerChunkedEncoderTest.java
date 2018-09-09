package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.http.TestUtils.AssertingBufsConsumer;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.test.TestUtils.assertComplete;

public class BufsConsumerChunkedEncoderTest {
	@Rule
	public final ByteBufRule byteBufRule = new ByteBufRule();
	private final Random RANDOM = new Random();
	private final ByteBufQueue queue = new ByteBufQueue();
	private final AssertingBufsConsumer consumer = new AssertingBufsConsumer();
	private final BufsConsumerChunkedEncoder encoder = new BufsConsumerChunkedEncoder(consumer);
	private final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());

	@Before
	public void setUp() {
		queue.recycle();
		consumer.reset();
	}

	@Test
	public void testEncoderSingleBuf() {
		byte[] chunkData = new byte[100];
		RANDOM.nextBytes(chunkData);
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		buf.put(chunkData);
		ByteBuf expected = ByteBufPool.allocate(200);
		expected.put((Integer.toHexString(100) + "\r\n").getBytes());
		expected.put(chunkData);
		expected.put("\r\n0\r\n\r\n".getBytes());
		consumer.setExpectedByteArray(expected.asArray());
		expected.recycle();
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf))
				.thenCompose($ -> encoder.push(queue, true))
				.whenComplete(assertComplete(Assert::assertTrue));
		future.complete(null);

		eventloop.run();
	}

	@Test
	public void testEncodeWithEmptyBuf() {
		byte[] chunkData = new byte[100];
		RANDOM.nextBytes(chunkData);
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		buf.put(chunkData);
		ByteBuf expected = ByteBufPool.allocate(200);
		ByteBuf empty = ByteBufPool.allocate(100);
		expected.put((Integer.toHexString(100) + "\r\n").getBytes());
		expected.put(chunkData);
		expected.put("\r\n0\r\n\r\n".getBytes());
		consumer.setExpectedByteArray(expected.asArray());
		expected.recycle();
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf))
				.thenCompose($ -> encoder.push(queue, false))
				.thenRun(() -> queue.add(empty))
				.thenCompose($ -> encoder.push(queue, true))
				.whenComplete(assertComplete(Assert::assertTrue));
		future.complete(null);

		eventloop.run();
	}

	@Test
	public void testWithChunkedOutputStream() throws IOException {
		byte[] chunkData = new byte[1000];
		RANDOM.nextBytes(chunkData);
		consumer.setExpectedByteArray(BufsConsumerChunkedDecoderTest.encode(chunkData, true));
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		buf.put(chunkData);
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf))
				.thenCompose($ -> encoder.push(queue, true))
				.whenComplete(assertComplete(Assert::assertTrue));
		future.complete(null);

		eventloop.run();
	}
}
