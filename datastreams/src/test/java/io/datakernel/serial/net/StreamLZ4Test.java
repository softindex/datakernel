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

package io.datakernel.serial.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.datakernel.serial.processor.SerialByteChunker;
import io.datakernel.serial.processor.SerialLZ4Compressor;
import io.datakernel.serial.processor.SerialLZ4Decompressor;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertArrayEquals;

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
		ByteBufQueue queue = new ByteBufQueue();
		for (ByteBuf buf : byteBufs) {
			queue.add(buf.slice());
		}
		byte[] bytes = new byte[queue.remainingBytes()];
		queue.drainTo(bytes, 0, bytes.length);
		return bytes;
	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 100;
		for (int i = 0; i < buffersCount; i++) {
			buffers.add(createRandomByteBuf(random));
		}
		byte[] expected = byteBufsToByteArray(buffers);

		CompletableFuture<List<ByteBuf>> future = SerialSupplier.ofIterable(buffers)
				.with(SerialByteChunker.create(MemSize.of(64), MemSize.of(128)).transformer(new SerialZeroBuffer<>()))
				.with(SerialLZ4Compressor.createFastCompressor().transformer(new SerialZeroBuffer<>()))
				.with(SerialByteChunker.create(MemSize.of(64), MemSize.of(128)).transformer(new SerialZeroBuffer<>()))
				.with(SerialLZ4Decompressor.create().transformer(new SerialZeroBuffer<>()))
				.toList()
				.toCompletableFuture();

		eventloop.run();

		byte[] actual = byteBufsToByteArray(future.get());
		for (ByteBuf buf : future.get()) {
			buf.recycle();
		}

		assertArrayEquals(expected, actual);
	}

	@Test
	public void testLz4Fast() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		doTest(eventloop, SerialLZ4Compressor.createFastCompressor());
	}

	@Test
	public void testLz4High() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		doTest(eventloop, SerialLZ4Compressor.createHighCompressor());
	}

	@Test
	public void testLz4High10() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		doTest(eventloop, SerialLZ4Compressor.createHighCompressor(10));
	}

	private void doTest(Eventloop eventloop, SerialLZ4Compressor compressor) throws Exception {
		byte data[] = "1".getBytes();
		ByteBuf buf = ByteBuf.wrapForReading(data);
		List<ByteBuf> buffers = new ArrayList<>();
		buffers.add(buf);

		CompletableFuture<List<ByteBuf>> result = SerialSupplier.ofIterable(buffers)
				.with(compressor.transformer(new SerialZeroBuffer<>()))
				.with(SerialLZ4Decompressor.create().transformer(new SerialZeroBuffer<>()))
				.toList()
				.toCompletableFuture();

		eventloop.run();

		byte[] actual = byteBufsToByteArray(result.get());
		byte[] expected = byteBufsToByteArray(buffers);
		for (ByteBuf b : result.get()) {
			b.recycle();
		}
		assertArrayEquals(actual, expected);
	}

}
