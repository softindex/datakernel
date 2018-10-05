package io.datakernel.http.stream;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.http.TestUtils.AssertingConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.Deflater;

import static io.datakernel.bytebuf.ByteBuf.wrapForReading;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.serial.ByteBufsSupplier.UNEXPECTED_DATA_EXCEPTION;
import static java.lang.Math.min;
import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BufsConsumerGzipInflaterTest {
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
	public final Random random = new Random();
	public final List<ByteBuf> list = new ArrayList<>();
	public final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	public final AssertingConsumer consumer = new AssertingConsumer();
	public BufsConsumerGzipInflater gunzip = BufsConsumerGzipInflater.create();

	@Before
	public void setUp() {
		consumer.reset();
		gunzip.getOutput().set(consumer);
	}

	@Test
	public void testForPlaintextMultipleBufs() {
		StringBuilder builder = new StringBuilder();
		for (String s : plainText) {
			builder.append(s);
		}
		consumer.setExpectedByteArray(builder.toString().getBytes());

		byte[] deflated = deflate(builder.toString().getBytes());
		int chunk = 10;
		for (int i = 0; i < deflated.length; i += chunk) {
			byte[] bytes = copyOfRange(deflated, i, min(deflated.length, i + chunk));
			ByteBuf buf = ByteBufPool.allocate(bytes.length);
			buf.put(bytes);
			list.add(buf);
		}

		doTest(null);
	}

	@Test
	public void testWithBufsgreaterThanBuffer() {
		String text = generateLargeText();
		consumer.setExpectedByteArray(text.getBytes());
		byte[] deflated = deflate(text.getBytes());
		int chunk = new Random().nextInt(1000) + 512;
		for (int i = 0; i < deflated.length; i += chunk) {
			byte[] bytes = copyOfRange(deflated, i, min(deflated.length, i + chunk));
			ByteBuf buf = ByteBufPool.allocate(bytes.length);
			buf.put(bytes);
			list.add(buf);
		}

		doTest(null);
	}

	@Test
	public void shouldCorrectlyProcessHeader() {
		byte flag = (byte) 0b00011111;
		short fextra = 5;
		short fextraReversed = Short.reverseBytes(fextra);
		byte feXtra1 = (byte) (fextraReversed >> 8);
		byte feXtra2 = (byte) (fextraReversed);

		byte[] header = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, flag, 0, 0, 0, 0, 0, 0,
				// FEXTRA PART
				feXtra1, feXtra2, 1, 2, 3, 4, 5,
				// FNAME PART
				1, 2, 3, 4, 5, 6, 7, 8, 0,
				// COMMENT PART
				1, 2, 3, 4, 5, 6, 7, 8, 0,
				// FHCRC PART
				123, 123
		};
		byte[] bytes = {1, 2, 3};
		ByteBuf gzipped = toGzip(wrapForReading(bytes));
		ByteBuf buf = ByteBufPool.allocate(gzipped.readRemaining() + header.length);
		buf.put(header);
		buf.put(gzipped.array(), 10, gzipped.readRemaining()-10);
		gzipped.recycle();
		list.add(buf);
		consumer.setExpectedByteArray(bytes);

		doTest(null);
	}

	@Test
	public void testForPlaintextSingleBuf() {
		StringBuilder finalMessage = new StringBuilder();
		for (String s : plainText) {
			finalMessage.append(s);
		}
		consumer.setExpectedByteArray(finalMessage.toString().getBytes());

		byte[] bytes = deflate(finalMessage.toString().getBytes());
		ByteBuf buf = ByteBufPool.allocate(bytes.length);
		buf.put(bytes);
		list.add(buf);

		doTest(null);
	}

	@Test
	public void shouldThrowExceptionIfAdditionalData() {
		StringBuilder finalMessage = new StringBuilder();
		for (String s : plainText) {
			finalMessage.append(s);
		}
		consumer.setExpectedByteArray(finalMessage.toString().getBytes());
		consumer.setExpectedException(UNEXPECTED_DATA_EXCEPTION);

		byte[] data = deflate(finalMessage.toString().getBytes());
		byte[] withExtraData = new byte[data.length + 3];
		withExtraData[data.length] = 100;
		withExtraData[data.length + 1] = -100;
		withExtraData[data.length + 2] = 126;
		System.arraycopy(data, 0, withExtraData, 0, data.length);
		ByteBuf buf = ByteBufPool.allocate(withExtraData.length);
		buf.put(withExtraData);
		list.add(buf);

		doTest(UNEXPECTED_DATA_EXCEPTION);
	}

	@Test
	public void testWithEmptyBuf() {
		String message = "abcd";
		consumer.setExpectedByteArray(message.getBytes());
		byte[] deflated = deflate(message.getBytes());
		byte[] partOne = copyOfRange(deflated, 0, 4);
		byte[] partTwo = copyOfRange(deflated, 4, deflated.length);
		ByteBuf buf1 = ByteBufPool.allocate(partOne.length);
		ByteBuf buf2 = ByteBufPool.allocate(partTwo.length);
		ByteBuf empty = ByteBufPool.allocate(100);
		buf1.put(partOne);
		buf2.put(partTwo);
		list.add(buf1);
		list.add(empty);
		list.add(buf2);

		doTest(null);
	}

	public byte[] deflate(byte[] array) {
		return toGzip(wrapForReading(array)).asArray();
	}

	// Test with GzipProcessorUtils Compatibility

	@Test
	public void testEncodeDecode() {
		String largeText = generateLargeText();
		ByteBuf raw = toGzip(wrapAscii(largeText));
		consumer.setExpectedString(largeText);
		list.add(raw.slice(100));
		raw.moveReadPosition(100);
		list.add(raw.slice());
		raw.recycle();

		doTest(null);
	}

	@Test
	public void testWithSingleBuf() {
		String largeText = generateLargeText();
		ByteBuf raw = toGzip(wrapAscii(largeText));
		consumer.setExpectedString(largeText);
		list.add(raw);

		doTest(null);
	}

	private void doTest(Exception exception) {
		gunzip.getInput().set(SerialSupplier.ofIterable(list));
		eventloop.post(() -> gunzip.start().whenComplete(($, e) -> {
			if (exception == null) {
				assertNull(e);
			} else {
				assertEquals(exception, e);
			}
		}));

		eventloop.run();
	}

	private static String generateLargeText() {
		Random charRandom = new Random(1L);
		int charactersCount = 100_000;
		StringBuilder sb = new StringBuilder(charactersCount);
		for (int i = 0; i < charactersCount; i++) {
			int charCode = charRandom.nextInt(255);
			sb.append((char) charCode);
		}
		return sb.toString();
	}

}
