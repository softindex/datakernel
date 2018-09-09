package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import sun.net.www.http.ChunkedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.http.TestUtils.AssertingBufsConsumer;
import static io.datakernel.http.stream.BufsConsumerChunkedDecoder.CHUNK_SIZE_EXCEEDS_MAXIMUM_SIZE;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.lang.System.arraycopy;
import static org.junit.Assert.*;

public class BufsConsumerChunkedDecoderTest {
	@Rule
	public ByteBufRule rule = new ByteBufRule();
	public final String[] plainText = {
			"Suspendisse faucibus enim curabitur tempus leo viverra massa accumsan nisl nunc\n",
			"Interdum sapien vehicula\nOrnare odio feugiat fringilla ",
			"Auctor sodales elementum curabitur felis ut ",
			"Ante sem orci rhoncus hendrerit commo",
			"do, potenti cursus lorem ac pretium, netus\nSapien hendrerit leo ",
			"Mollis volutpat nisi convallis accumsan eget praesent cursus urna ultricies nec habitasse nam\n",
			"Inceptos nisl magna duis vel taciti volutpat nostra\n",
			"Taciti sapien fringilla u\nVitae ",
			"Etiam egestas ac augue dui dapibus, aliquam adipiscing porttitor magna at, libero elit faucibus purus"
	};
	public final AssertingBufsConsumer consumer = new AssertingBufsConsumer();
	public final ByteBufQueue queue = new ByteBufQueue();
	public final Random random = new Random();
	public final BufsConsumerChunkedDecoder chunkedDecoder = new BufsConsumerChunkedDecoder(consumer);
	public final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());

	@Before
	public void setUp() {
		queue.recycle();
		consumer.reset();
	}

	@Test
	public void testDecoderWithStrings() throws IOException {
		StringBuilder builder = new StringBuilder();
		for (String s : plainText) {
			builder.append(s);
		}
		String finalString = builder.toString();
		consumer.setExpectedByteArray(finalString.getBytes());

		for (int i = 0; i < plainText.length; i++) {
			boolean lastchunk = i == plainText.length - 1;
			byte[] encoded = encode(plainText[i].getBytes(), lastchunk);
			ByteBuf buf = ByteBufPool.allocate(encoded.length);
			buf.put(encoded);
			queue.add(buf);
			chunkedDecoder.push(queue, lastchunk)
					.whenComplete(assertComplete(r -> {
						if (lastchunk) {
							assertTrue(r);
						}
					}));
		}
	}

	@Test
	public void testDecoderWithRandomData() throws IOException {
		List<byte[]> randomData = new ArrayList<>();
		int size = random.nextInt(100) + 50;
		int totalBytes = 0;
		for (int i = 0; i < size; i++) {
			int bytes = random.nextInt(1000) + 1;
			totalBytes += bytes;
			byte[] data = new byte[bytes];
			random.nextBytes(data);
			randomData.add(data);
		}

		byte[] expected = new byte[totalBytes];
		for (int i = 0; i < totalBytes; ) {
			for (byte[] bytes : randomData) {
				int length = bytes.length;
				arraycopy(bytes, 0, expected, i, length);
				i += length;
			}
		}
		consumer.setExpectedByteArray(expected);

		for (int i = 0; i < size; i++) {
			boolean lastchunk = i == size - 1;
			byte[] encoded = encode(randomData.get(i), lastchunk);
			ByteBuf buf = ByteBufPool.allocate(encoded.length);
			buf.put(encoded);
			queue.add(buf);
			chunkedDecoder.push(queue, lastchunk)
					.whenComplete(assertComplete(r -> {
						if (lastchunk) {
							assertTrue(r);
						}
					}));
		}
	}

	@Test
	public void itShouldIgnoreChunkExtAfterNotLastChunk() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5;name=value\r\nabcde\r\n0\r\n\r\n";
		decodeOneString(message, false);
	}

	@Test
	public void itShouldIgnoreChunkExtAfterLastChunk() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5\r\nabcde\r\n0;name=value\r\n\r\n";
		decodeOneString(message, false);
	}

	@Test
	public void itShouldIgnoreChunkExtAfterChunkInAnotherBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0";
		String message2 = ";name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void itShouldIgnoreChunkExtAfterChunkSemicolonInSameBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0;";
		String message2 = "name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void itShouldWorkWithSizeCRLFInNextBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5";
		String message2 = "\r\nabcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void itShouldWorkWithSizeCRLFInSameBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\n";
		String message2 = "abcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void itShouldWorkWithCRLFInDifferentBufs() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5;abcd\r";
		String message2 = "\nabcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void testForSplittedChunkSize() {
		consumer.setExpectedByteArray("1234567890123456789".getBytes());
		String message1 = "1";
		String message2 = "3;asdasdasdasd\r\n";
		String message3 = "1234567890123456789\r\n0\r\n\r\n";
		decodeThreeStrings(message1, message2, message3);
	}

	@Test
	public void shouldThrowChunkSizeException() {
		consumer.setExpectedException(CHUNK_SIZE_EXCEEDS_MAXIMUM_SIZE);
		String message = Integer.toHexString(1025);
		decodeOneString(message, true);
	}

	@Test
	public void itShouldIgnoreTrailerPart() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5\r\nabcde\r\n0\r\ntrailer1\r\ntrailer2\r\n\r\n";
		decodeOneString(message, false);
	}

	@Test
	public void shouldIgnoreTrailerPartInMultipleBufs() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0\r\ntra";
		String message2 = "iler1\r\ntra";
		String message3 = "iler2\r\n\r\n";
		decodeThreeStrings(message1, message2, message3);
	}

	@Test
	public void shouldIgnoreTrailerPartInDifferentBufs() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0\r\ntra";
		String message2 = "iler1\r\ntrailer2\r\n";
		String message3 = "trailer3\r\n\r\n";
		decodeThreeStrings(message1, message2, message3);
	}

	private void decodeOneString(String message, boolean exception) {
		byte[] bytes = message.getBytes();
		ByteBuf buf = ByteBufPool.allocate(bytes.length);
		buf.put(bytes);
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf))
				.thenCompose($ -> chunkedDecoder.push(queue, true))
				.whenComplete((r, e) -> {
					if (exception) {
						assertNotNull(e);
					} else {
						assertNull(e);
						assertTrue(r);
					}
				});
		future.complete(null);
		eventloop.run();
	}

	private void decodeTwoStrings(String message1, String message2) {
		byte[] bytes1 = message1.getBytes();
		byte[] bytes2 = message2.getBytes();
		ByteBuf buf1 = ByteBufPool.allocate(bytes1.length);
		ByteBuf buf2 = ByteBufPool.allocate(bytes2.length);
		buf1.put(bytes1);
		buf2.put(bytes2);
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf1))
				.thenCompose($ -> chunkedDecoder.push(queue, false))
				.thenRun(() -> queue.add(buf2))
				.thenCompose($ -> chunkedDecoder.push(queue, true))
				.whenComplete(assertComplete(Assert::assertTrue));
		future.complete(null);
		eventloop.run();
	}

	private void decodeThreeStrings(String message1, String message2, String message3) {
		byte[] bytes1 = message1.getBytes();
		byte[] bytes2 = message2.getBytes();
		byte[] bytes3 = message3.getBytes();
		ByteBuf buf1 = ByteBufPool.allocate(bytes1.length);
		ByteBuf buf2 = ByteBufPool.allocate(bytes2.length);
		ByteBuf buf3 = ByteBufPool.allocate(bytes3.length);
		buf1.put(bytes1);
		buf2.put(bytes2);
		buf3.put(bytes3);
		CompletableFuture<Void> future = new CompletableFuture<>();
		Stage.ofCompletionStage(future)
				.thenRun(() -> queue.add(buf1))
				.thenCompose($ -> chunkedDecoder.push(queue, false))
				.thenRun(() -> queue.add(buf2))
				.thenCompose($ -> chunkedDecoder.push(queue, false))
				.thenRun(() -> queue.add(buf3))
				.thenCompose($ -> chunkedDecoder.push(queue, true))
				.whenComplete(assertComplete(Assert::assertTrue));
		future.complete(null);
		eventloop.run();
	}


	public static byte[] encode(byte[] data, boolean lastchunk) throws IOException {
		try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
			ChunkedOutputStream zip = new ChunkedOutputStream(new PrintStream(stream));
			zip.write(data);
			zip.flush();
			if (lastchunk) {
				zip.close();
				// making the last block end with 0\r\n\r\n
				stream.write(13);
				stream.write(10);
			}
			return stream.toByteArray();
		}
	}
}
