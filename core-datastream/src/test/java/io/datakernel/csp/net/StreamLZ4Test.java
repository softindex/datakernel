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

package io.datakernel.csp.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.MemSize;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelByteChunker;
import io.datakernel.csp.process.ChannelLZ4Compressor;
import io.datakernel.csp.process.ChannelLZ4Decompressor;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.datakernel.promise.TestUtils.await;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;

public final class StreamLZ4Test {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		//[START EXAMPLE]
		int buffersCount = 100;

		List<ByteBuf> buffers = IntStream.range(0, buffersCount).mapToObj($ -> createRandomByteBuf()).collect(toList());
		byte[] expected = buffers.stream().map(ByteBuf::slice).collect(ByteBufQueue.collector()).asArray();

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofIterable(buffers)
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelLZ4Compressor.createFastCompressor())
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelLZ4Decompressor.create());

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(expected, collected.asArray());
		//[END EXAMPLE]
	}

	@Test
	public void testLz4Fast() {
		doTest(ChannelLZ4Compressor.createFastCompressor());
	}

	@Test
	public void testLz4High() {
		doTest(ChannelLZ4Compressor.createHighCompressor());
	}

	@Test
	public void testLz4High10() {
		doTest(ChannelLZ4Compressor.createHighCompressor(10));
	}

	private void doTest(ChannelLZ4Compressor compressor) {
		byte[] data = "1".getBytes();

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(ByteBuf.wrapForReading(data))
				.transformWith(compressor)
				.transformWith(ChannelLZ4Decompressor.create());

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(data, collected.asArray());
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
