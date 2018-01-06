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
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertStatus;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class StreamLZ4Test {
	private static ByteBuf createRandomByteBuf(Random random) {
		int offset = random.nextInt(10);
		int tail = random.nextInt(10);
		int len = random.nextInt(100);
		ByteBuf result = ByteBuf.wrapForWriting(new byte[offset + len + tail]);
		int lenUnique = 1 + random.nextInt(len + 1);
		result.writePosition(offset);
		result.readPosition(offset);
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 100;
		for (int i = 0; i < buffersCount; i++) {
			buffers.add(createRandomByteBuf(random));
		}
		byte[] expected = byteBufsToByteArray(buffers);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(buffers);
		StreamByteChunker preBuf = StreamByteChunker.create(64, 128);
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor();
		StreamByteChunker postBuf = StreamByteChunker.create(64, 128);
		StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create();
		StreamConsumerToList<ByteBuf> consumer = StreamConsumerToList.randomlySuspending();

//		source.streamTo(compressor.getInput());
		stream(source, preBuf.getInput());
		stream(preBuf.getOutput(), compressor.getInput());

//		compressor.getOutput().streamTo(decompressor.getInput());
		stream(compressor.getOutput(), postBuf.getInput());
		stream(postBuf.getOutput(), decompressor.getInput());

		stream(decompressor.getOutput(), consumer);

		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		for (ByteBuf buf : consumer.getList()) {
			buf.recycle();
		}

		assertArrayEquals(expected, actual);
		assertStatus(END_OF_STREAM, source);

//		assertStatus(END_OF_STREAM, preBuf.getInput());
//		assertStatus(END_OF_STREAM, preBuf.getOutput());

		assertStatus(END_OF_STREAM, compressor.getInput());
		assertStatus(END_OF_STREAM, compressor.getOutput());

//		assertStatus(END_OF_STREAM, postBuf.getInput());
//		assertStatus(END_OF_STREAM, postBuf.getOutput());

		assertStatus(END_OF_STREAM, decompressor.getInput());
		assertStatus(END_OF_STREAM, decompressor.getOutput());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 1000;
		for (int i = 0; i < buffersCount; i++) {
			ByteBuf buffer = createRandomByteBuf(random);
			buffers.add(buffer);
		}
		byte[] expected = byteBufsToByteArray(buffers);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(buffers);
		StreamByteChunker preBuf = StreamByteChunker.create(64, 128);
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor();
		StreamByteChunker postBuf = StreamByteChunker.create(64, 128);
		StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create();
		StreamConsumerToList<ByteBuf> consumer = StreamConsumerToList.randomlySuspending();

		stream(source, preBuf.getInput());
		eventloop.run();

		stream(preBuf.getOutput(), compressor.getInput());
		eventloop.run();

		stream(compressor.getOutput(), postBuf.getInput());
		eventloop.run();

		stream(postBuf.getOutput(), decompressor.getInput());
		eventloop.run();

		stream(decompressor.getOutput(), consumer);
		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		for (ByteBuf buf : consumer.getList()) {
			buf.recycle();
		}

		assertArrayEquals(expected, actual);
		assertStatus(END_OF_STREAM, source);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		assertStatus(END_OF_STREAM, preBuf.getInput());
		assertStatus(END_OF_STREAM, preBuf.getOutput());

		assertStatus(END_OF_STREAM, compressor.getInput());
		assertStatus(END_OF_STREAM, compressor.getOutput());

		assertStatus(END_OF_STREAM, postBuf.getInput());
		assertStatus(END_OF_STREAM, postBuf.getOutput());

		assertStatus(END_OF_STREAM, decompressor.getInput());
		assertStatus(END_OF_STREAM, decompressor.getOutput());
	}

	@Test
	public void testRaw() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		StreamLZ4Compressor compressor = StreamLZ4Compressor.rawCompressor();

		doTest(eventloop, compressor);
	}

	@Test
	public void testLz4Fast() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor();

		doTest(eventloop, compressor);
	}

	@Test
	public void testLz4High() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		StreamLZ4Compressor compressor = StreamLZ4Compressor.highCompressor();

		doTest(eventloop, compressor);
	}

	@Test
	public void testLz4High10() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		StreamLZ4Compressor compressor = StreamLZ4Compressor.highCompressor(10);

		doTest(eventloop, compressor);
	}

	private void doTest(Eventloop eventloop, StreamLZ4Compressor compressor) {
		byte data[] = "1".getBytes();
		ByteBuf buf = ByteBuf.wrapForReading(data);
		List<ByteBuf> buffers = new ArrayList<>();
		buffers.add(buf);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(buffers);
		StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create();
		StreamConsumerToList<ByteBuf> consumer = StreamConsumerToList.create();

		stream(source, compressor.getInput());
		stream(compressor.getOutput(), decompressor.getInput());
		stream(decompressor.getOutput(), consumer);

		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());
		byte[] expected = byteBufsToByteArray(buffers);
		for (ByteBuf b : consumer.getList()) {
			b.recycle();
		}
		assertArrayEquals(actual, expected);

		assertStatus(END_OF_STREAM, source);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

}
