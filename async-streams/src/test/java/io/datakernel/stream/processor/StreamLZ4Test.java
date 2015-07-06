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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

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
		int size = 0;
		for (ByteBuf byteBuf : byteBufs) {
			size += byteBuf.remaining();
		}
		byte[] result = new byte[size];
		int pos = 0;
		for (ByteBuf byteBuf : byteBufs) {
			System.arraycopy(byteBuf.array(), byteBuf.position(),
					result, pos, byteBuf.remaining());
			pos += byteBuf.remaining();
		}
		return result;
	}

	@Test
	public void test() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 50;
		for (int i = 0; i < buffersCount; i++) {
			ByteBuf buffer = createRandomByteBuf(random);
			buffers.add(buffer);
		}
		byte[] expected = byteBufsToByteArray(buffers);

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamLZ4Compressor compressor = new StreamLZ4Compressor(eventloop, 64, 32);
		StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		StreamConsumers.ToList<ByteBuf> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(compressor);

		compressor.streamTo(decompressor);
		decompressor.streamTo(consumer);

		eventloop.run();

		byte[] actual = byteBufsToByteArray(consumer.getList());

		assertArrayEquals(expected, actual);
		assertTrue(source.getStatus() == StreamProducer.CLOSED);
	}

}
