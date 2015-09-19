/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.stream.processor;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static org.junit.Assert.*;

public class StreamLZ4Test {

	private static ByteBuf createRandomByteBuf(Random random) {
		int offset = random.nextInt(10);
		int len = random.nextInt(100);
		int tail = random.nextInt(10);
		ByteBuf result = ByteBuf.allocate(offset + len + tail);
		result.position(offset);
		result.limit(offset + len);
		int lenUnique = 1 + random.nextInt(len + 1);
		for (int i = 0; i < len; i++) {
			result.array()[offset + i] = (byte) (i % lenUnique);
		}
		return result;
	}

	private static byte[] byteBufsToByteArray(List<ByteBuf> byteBufs) {
		ByteBufQueue queue = new ByteBufQueue();
		for (ByteBuf buf : byteBufs) {
			queue.add(buf.slice(0, buf.remaining()));
		}
		byte[] bytes = new byte[queue.remainingBytes()];
		queue.drainTo(bytes, 0, bytes.length);
		return bytes;
	}

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test() {
		NioEventloop eventloop = new NioEventloop();

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 1000;
		for (int i = 0; i < buffersCount; i++) {
			ByteBuf buffer = createRandomByteBuf(random);
			buffer.flip();
			buffers.add(buffer);
		}
		byte[] expected = byteBufsToByteArray(buffers);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamByteChunker preBuf = new StreamByteChunker(eventloop, 64, 128);
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamByteChunker postBuf = new StreamByteChunker(eventloop, 64, 128);
		StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		StreamConsumers.ToList<ByteBuf> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(preBuf);
		preBuf.streamTo(compressor);
		compressor.streamTo(postBuf);
		postBuf.streamTo(decompressor);
		decompressor.streamTo(consumer);

		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		for (ByteBuf buf : consumer.getList()) {
			buf.recycle();
		}

		assertArrayEquals(expected, actual);
		assertTrue(((AbstractStreamProducer)source).getStatus() == AbstractStreamProducer.END_OF_STREAM);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testRaw() {
		NioEventloop eventloop = new NioEventloop();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.rawCompressor(eventloop);

		doTest(eventloop, compressor);
	}

	@Test
	public void testLz4Fast() {
		NioEventloop eventloop = new NioEventloop();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);

		doTest(eventloop, compressor);
	}

	@Test
	public void testLz4High() {
		NioEventloop eventloop = new NioEventloop();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.highCompressor(eventloop);

		doTest(eventloop, compressor);
	}

	@Test
	public void testLz4High10() {
		NioEventloop eventloop = new NioEventloop();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.highCompressor(eventloop, 10);

		doTest(eventloop, compressor);
	}

	private void doTest(NioEventloop eventloop, StreamLZ4Compressor compressor) {
		byte data[] = "1".getBytes();
		ByteBuf buf = ByteBuf.allocate(data.length);
		buf.put(data);
		buf.flip();
		List<ByteBuf> buffers = new ArrayList<>();
		buffers.add(buf);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		StreamConsumers.ToList<ByteBuf> consumer = StreamConsumers.toList(eventloop);

		source.streamTo(compressor);
		compressor.streamTo(decompressor);
		decompressor.streamTo(consumer);

		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		byte[] expected = byteBufsToByteArray(buffers);
		for (ByteBuf b : consumer.getList()) {
			b.recycle();
		}
		assertArrayEquals(actual, expected);

		assertTrue(((AbstractStreamProducer)source).getStatus() == AbstractStreamProducer.END_OF_STREAM);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}
