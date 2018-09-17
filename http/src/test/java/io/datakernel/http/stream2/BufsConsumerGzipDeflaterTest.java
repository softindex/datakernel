package io.datakernel.http.stream2;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.test.TestUtils.assertComplete;

public class BufsConsumerGzipDeflaterTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	public final AssertingConsumer consumer = new AssertingConsumer();
	public BufsConsumerGzipDeflater gzip = BufsConsumerGzipDeflater.create();
	public final ArrayList<ByteBuf> list = new ArrayList<>();
	public final Random random = new Random();
	public final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());

	@Before
	public void setUp() {
		consumer.reset();
		gzip.setOutput(consumer);
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

		for (String string : strings) {
			ByteBuf buf = ByteBufPool.allocate(string.length());
			buf.put(string.getBytes());
			list.add(buf);
		}

		doTest();
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
		list.add(buf2);
		ByteBuf expected = toGzip(buf);
		consumer.setExpectedBuf(expected);
		doTest();
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
			list.add(buf2.slice(bufSize));
			buf2.moveReadPosition(bufSize);
		}
		buf2.recycle();

		doTest();
	}

	private void doTest() {
		gzip.setInput(SerialSupplier.ofIterable(list));
		eventloop.post(() -> gzip.start().whenComplete(assertComplete()));

		eventloop.run();
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
