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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static org.junit.Assert.*;

public class StreamLZ4Test {
	private static ByteBuf createRandomByteBuf(Random random) {
		int offset = random.nextInt(10);
		int tail = random.nextInt(10);
		int len = random.nextInt(100);
		ByteBuf result = ByteBuf.wrapForWriting(new byte[offset + len + tail]);
		int lenUnique = 1 + random.nextInt(len + 1);
		result.tail(offset);
		result.head(offset);
		for (int i = 0; i < len; i++) {
			result.put((byte) (i % lenUnique));
		}
		return result;
	}

	private static byte[] byteBufsToByteArray(List<ByteBuf> byteBufs) {
		ByteBufQueue queue = ByteBufQueue.create();
		for (ByteBuf buf : byteBufs) {
			queue.add(buf.slice());
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
		Eventloop eventloop = Eventloop.create();

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 1000;
		for (int i = 0; i < buffersCount; i++) {
			buffers.add(createRandomByteBuf(random));
		}
		byte[] expected = byteBufsToByteArray(buffers);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamByteChunker preBuf = StreamByteChunker.create(eventloop, 64, 128);
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamByteChunker postBuf = StreamByteChunker.create(eventloop, 64, 128);
		StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);
		TestStreamConsumers.TestConsumerToList<ByteBuf> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(preBuf.getInput());
		preBuf.getOutput().streamTo(compressor.getInput());
		compressor.getOutput().streamTo(postBuf.getInput());
		postBuf.getOutput().streamTo(decompressor.getInput());
		decompressor.getOutput().streamTo(consumer);

		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		for (ByteBuf buf : consumer.getList()) {
			buf.recycle();
		}

		assertArrayEquals(expected, actual);
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		assertEquals(END_OF_STREAM, preBuf.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, preBuf.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, compressor.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, compressor.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, postBuf.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, postBuf.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, decompressor.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, decompressor.getOutput().getProducerStatus());

		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 1000;
		for (int i = 0; i < buffersCount; i++) {
			ByteBuf buffer = createRandomByteBuf(random);
			buffers.add(buffer);
		}
		byte[] expected = byteBufsToByteArray(buffers);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamByteChunker preBuf = StreamByteChunker.create(eventloop, 64, 128);
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamByteChunker postBuf = StreamByteChunker.create(eventloop, 64, 128);
		StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);
		TestStreamConsumers.TestConsumerToList<ByteBuf> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(preBuf.getInput());
		eventloop.run();

		preBuf.getOutput().streamTo(compressor.getInput());
		eventloop.run();

		compressor.getOutput().streamTo(postBuf.getInput());
		eventloop.run();

		postBuf.getOutput().streamTo(decompressor.getInput());
		eventloop.run();

		decompressor.getOutput().streamTo(consumer);
		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		for (ByteBuf buf : consumer.getList()) {
			buf.recycle();
		}

		assertArrayEquals(expected, actual);
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		assertEquals(END_OF_STREAM, preBuf.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, preBuf.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, compressor.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, compressor.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, postBuf.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, postBuf.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, decompressor.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, decompressor.getOutput().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testRaw() {
		Eventloop eventloop = Eventloop.create();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.rawCompressor(eventloop);

		doTest(eventloop, compressor);
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testLz4Fast() {
		Eventloop eventloop = Eventloop.create();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);

		doTest(eventloop, compressor);
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testLz4High() {
		Eventloop eventloop = Eventloop.create();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.highCompressor(eventloop);

		doTest(eventloop, compressor);
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testLz4High10() {
		Eventloop eventloop = Eventloop.create();
		StreamLZ4Compressor compressor = StreamLZ4Compressor.highCompressor(eventloop, 10);

		doTest(eventloop, compressor);
		assertThat(eventloop, doesntHaveFatals());
	}

	private void doTest(Eventloop eventloop, StreamLZ4Compressor compressor) {
		byte data[] = "1".getBytes();
		ByteBuf buf = ByteBuf.wrapForReading(data);
		List<ByteBuf> buffers = new ArrayList<>();
		buffers.add(buf);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);
		StreamConsumers.ToList<ByteBuf> consumer = StreamConsumers.toList(eventloop);

		source.streamTo(compressor.getInput());
		compressor.getOutput().streamTo(decompressor.getInput());
		decompressor.getOutput().streamTo(consumer);

		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		byte[] expected = byteBufsToByteArray(buffers);
		for (ByteBuf b : consumer.getList()) {
			b.recycle();
		}
		assertArrayEquals(actual, expected);

		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

}
