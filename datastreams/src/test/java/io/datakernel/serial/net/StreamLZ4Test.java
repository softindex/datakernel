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

package io.datakernel.serial.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.SerialByteChunker;
import io.datakernel.serial.processor.SerialLZ4Compressor;
import io.datakernel.serial.processor.SerialLZ4Decompressor;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.MemSize;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;

@RunWith(DatakernelRunner.class)
public final class StreamLZ4Test {

	@Test
	public void test() {
		int buffersCount = 100;

		List<ByteBuf> buffers = IntStream.range(0, buffersCount).mapToObj($ -> createRandomByteBuf()).collect(toList());
		byte[] expected = buffers.stream().map(ByteBuf::slice).collect(ByteBufQueue.collector()).asArray();

		SerialSupplier.ofIterable(buffers)
				.apply(SerialByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.apply(SerialLZ4Compressor.createFastCompressor())
				.apply(SerialByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.apply(SerialLZ4Decompressor.create())
				.toCollector(ByteBufQueue.collector())
				.whenComplete(assertComplete(buf -> assertArrayEquals(expected, buf.asArray())));
	}

	@Test
	public void testLz4Fast() {
		doTest(SerialLZ4Compressor.createFastCompressor());
	}

	@Test
	public void testLz4High() {
		doTest(SerialLZ4Compressor.createHighCompressor());
	}

	@Test
	public void testLz4High10() {
		doTest(SerialLZ4Compressor.createHighCompressor(10));
	}

	private void doTest(SerialLZ4Compressor compressor) {
		byte[] data = "1".getBytes();

		SerialSupplier.of(ByteBuf.wrapForReading(data))
				.apply(compressor)
				.apply(SerialLZ4Decompressor.create())
				.toCollector(ByteBufQueue.collector())
				.whenComplete(assertComplete(buf -> assertArrayEquals(data, buf.asArray())));
	}

	private static ByteBuf createRandomByteBuf() {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		int offset = random.nextInt(10);
		int tail = random.nextInt(10);
		int len = random.nextInt(100);
		byte[] array = new byte[offset + len + tail];
		random.nextBytes(array);
		return ByteBuf.wrap(array, offset, offset + len);
	}
}
