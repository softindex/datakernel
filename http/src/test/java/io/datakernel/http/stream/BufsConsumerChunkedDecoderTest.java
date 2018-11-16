/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.http.stream;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import sun.net.www.http.ChunkedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.http.TestUtils.AssertingConsumer;
import static io.datakernel.http.stream.BufsConsumerChunkedDecoder.MALFORMED_CHUNK;
import static io.datakernel.http.stream.BufsConsumerChunkedDecoder.MALFORMED_CHUNK_LENGTH;
import static java.lang.System.arraycopy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(DatakernelRunner.class)
public final class BufsConsumerChunkedDecoderTest {
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
	public final AssertingConsumer consumer = new AssertingConsumer();
	public final ByteBufQueue queue = new ByteBufQueue();
	public final List<ByteBuf> list = new ArrayList<>();
	public final BufsConsumerChunkedDecoder chunkedDecoder = BufsConsumerChunkedDecoder.create();
	public final Random random = new Random();

	@Before
	public void setUp() {
		queue.recycle();
		list.clear();
		consumer.reset();
		chunkedDecoder.getOutput().set(consumer);
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
			list.add(buf);
		}
		doTest(null);
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
			list.add(buf);
		}

		doTest(null);
	}

	@Test
	public void shouldIgnoreChunkExtAfterNotLastChunk() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5;name=value\r\nabcde\r\n0\r\n\r\n";
		decodeOneString(message, null);
	}

	@Test
	public void shouldIgnoreChunkExtAfterLastChunk() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5\r\nabcde\r\n0;name=value\r\n\r\n";
		decodeOneString(message, null);
	}

	@Test
	public void shouldIgnoreChunkExtAfterChunkInAnotherBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0";
		String message2 = ";name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldIgnoreChunkExtAfterChunkSemicolonInSameBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0;";
		String message2 = "name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldWorkWithSizeCRLFInNextBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5";
		String message2 = "\r\nabcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldWorkWithSizeCRLFInSameBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\n";
		String message2 = "abcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldWorkWithCRLFInDifferentBufs() {
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
		consumer.setExpectedException(MALFORMED_CHUNK_LENGTH);
		String message = Integer.toHexString(1025) + "\r\n";
		decodeOneString(message, MALFORMED_CHUNK_LENGTH);
	}

	@Test
	public void shouldThrowMalformedChunkException() {
		consumer.setExpectedException(MALFORMED_CHUNK);
		String message = Integer.toHexString(1);
		message += "\r\nssss\r\n";
		decodeOneString(message, MALFORMED_CHUNK);
	}

	@Test
	public void shouldIgnoreTrailerPart() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5\r\nabcde\r\n0\r\ntrailer1\r\ntrailer2\r\n\r\n";
		decodeOneString(message, null);
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

	private void decodeOneString(String message, @Nullable Exception e) {
		byte[] bytes = message.getBytes();
		ByteBuf buf = ByteBufPool.allocate(bytes.length);
		buf.put(bytes);
		list.add(buf);

		doTest(e);
	}

	private void decodeTwoStrings(String message1, String message2) {
		byte[] bytes1 = message1.getBytes();
		byte[] bytes2 = message2.getBytes();
		ByteBuf buf1 = ByteBufPool.allocate(bytes1.length);
		ByteBuf buf2 = ByteBufPool.allocate(bytes2.length);
		buf1.put(bytes1);
		buf2.put(bytes2);
		list.add(buf1);
		list.add(buf2);

		doTest(null);
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
		list.add(buf1);
		list.add(buf2);
		list.add(buf3);

		doTest(null);
	}

	private void doTest(@Nullable Exception expectedException) {
		chunkedDecoder.getInput().set(BinaryChannelSupplier.of(ChannelSupplier.ofIterable(list)));
		chunkedDecoder.getProcessResult()
				.whenComplete(($, e) -> {
					if (expectedException == null) {
						assertNull(e);
					} else {
						assertEquals(expectedException, e);
					}
				});
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
