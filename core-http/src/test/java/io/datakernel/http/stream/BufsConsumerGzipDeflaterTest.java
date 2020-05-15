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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.http.TestUtils.AssertingConsumer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPOutputStream;

import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;

public final class BufsConsumerGzipDeflaterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final AssertingConsumer consumer = new AssertingConsumer();
	private final BufsConsumerGzipDeflater gzip = BufsConsumerGzipDeflater.create();
	private final ArrayList<ByteBuf> list = new ArrayList<>();

	@Before
	public void setUp() {
		consumer.reset();
		gzip.getOutput().set(consumer);
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
		ThreadLocalRandom.current().nextBytes(data);
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
		ThreadLocalRandom.current().nextBytes(data);
		ByteBuf buf1 = ByteBufPool.allocate(data.length);
		ByteBuf buf2 = ByteBufPool.allocate(data.length);
		buf1.put(data);
		buf2.put(data);
		ByteBuf gzipped = toGzip(buf1);
		consumer.setExpectedBuf(gzipped);
		int bufSize = 100;
		for (int i = 0; i < data.length; i += bufSize) {
			list.add(buf2.slice(bufSize));
			buf2.moveHead(bufSize);
		}
		buf2.recycle();

		doTest();
	}

	@Test
	public void testCompressWithEmptyBufs() {
		byte[] data1 = new byte[100];
		byte[] data2 = new byte[100];
		byte[] data3 = new byte[100];

		ThreadLocalRandom.current().nextBytes(data1);
		ThreadLocalRandom.current().nextBytes(data2);
		ThreadLocalRandom.current().nextBytes(data3);

		ByteBuf buf1 = ByteBuf.wrapForReading(data1);
		ByteBuf buf2 = ByteBuf.wrapForReading(new byte[0]);
		ByteBuf buf3 = ByteBuf.wrapForReading(data2);
		ByteBuf buf4 = ByteBuf.wrapForReading(new byte[0]);
		ByteBuf buf5 = ByteBuf.wrapForReading(data3);
		ByteBuf buf6 = ByteBuf.wrapForReading(new byte[0]);

		ByteBuf fullBuf = ByteBufPool.allocate(500);
		fullBuf = ByteBufPool.append(fullBuf, buf1.slice());
		fullBuf = ByteBufPool.append(fullBuf, buf2.slice());
		fullBuf = ByteBufPool.append(fullBuf, buf3.slice());
		fullBuf = ByteBufPool.append(fullBuf, buf4.slice());
		fullBuf = ByteBufPool.append(fullBuf, buf5.slice());
		fullBuf = ByteBufPool.append(fullBuf, buf6.slice());

		ByteBuf gzipped = toGzip(fullBuf);
		consumer.setExpectedBuf(gzipped);

		list.addAll(asList(buf1, buf2, buf3, buf4, buf5, buf6));

		doTest();
	}

	private void doTest() {
		gzip.getInput().set(ChannelSupplier.ofIterable(list));
		await(gzip.getProcessCompletion());
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
