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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPOutputStream;

import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.test.TestUtils.assertComplete;
import static org.junit.Assert.assertTrue;

public class BufsConsumerGzipDeflaterTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	public final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	public final AssertingConsumer consumer = new AssertingConsumer();
	public BufsConsumerGzipDeflater gzip = new BufsConsumerGzipDeflater(consumer);
	public final ByteBufQueue queue = new ByteBufQueue();
	public final Random random = new Random();

	@Before
	public void setUp() {
		queue.recycle();
		consumer.reset();
	}

	@Test
	public void testCompress() {
		String[] strings = {"Hello ", "World", "!"};
		List<byte[]> arrays = new ArrayList<>();
		for (String s : strings) {
			arrays.add(s.getBytes());
		}
		byte[] expected = compressWithGzipOutputStream(arrays.toArray(new byte[0][0]));
		consumer.setExpectedByteArray(expected);

		for (int i = 0; i < strings.length; i++) {
			ByteBuf buf = ByteBufPool.allocate(strings[i].length());
			buf.put(strings[i].getBytes());
			queue.add(buf);
			boolean finish = i == strings.length - 1;
			gzip.push(queue, finish)
					.whenResult(r -> {
						if (finish) {
							assertTrue(r);
						}
					});
		}
	}

	// GzipParseUtils compatibility tests
	@Test
	public void testCompressSingleBuf() {
		byte[] data = new byte[17000];
		random.nextBytes(data);
		ByteBuf buf = ByteBufPool.allocate(data.length);
		ByteBuf buf2 = ByteBufPool.allocate(data.length);
		buf.put(data);
		buf2.put(data);
		ByteBuf temp = toGzip(buf);
		byte[] bytes = temp.asArray();
		temp.recycle();
		consumer.setExpectedByteArray(bytes);
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf2))
				.thenCompose($ -> gzip.push(queue, true))
				.whenComplete(assertComplete(Assert::assertTrue));
		future.complete(null);
		eventloop.run();
	}

	@Test
	public void testCompressMultipleBufs() {
		byte[] data = new byte[100_000];
		random.nextBytes(data);
		ByteBuf buf1 = ByteBufPool.allocate(data.length);
		ByteBuf buf2 = ByteBufPool.allocate(data.length);
		buf1.put(data);
		buf2.put(data);
		ByteBuf gzipped = toGzip(buf1);
		consumer.setExpectedBuf(gzipped);
		int bufSize = 100;
		for (int i = 0; i < data.length; i += bufSize) {
			queue.add(buf2.slice(bufSize));
			buf2.moveReadPosition(bufSize);
			boolean last = !buf2.canRead();
			gzip.push(queue, last)
					.whenComplete(assertComplete(r -> {
						if (last) {
							buf2.recycle();
							assertTrue(r);
						}
					}));
		}
	}

	private byte[] compressWithGzipOutputStream(byte[]... arrays) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(baos, true)) {
			for (int i = 0; i < arrays.length; i++) {
				gzip.write(arrays[i], 0, arrays[i].length);
				if (i == arrays.length - 1) {
					gzip.finish();
				}
			}
			return baos.toByteArray();
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

}
