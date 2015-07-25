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

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static org.junit.Assert.*;

public class StreamByteChunkerTest {

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	private static ByteBuf createRandomByteBuf(Random random) {
		int len = random.nextInt(100);
		ByteBuf result = ByteBuf.allocate(len);
		result.position(0);
		result.limit(len);
		int lenUnique = 1 + random.nextInt(len + 1);
		for (int i = 0; i < len; i++) {
			result.array()[i] = (byte) (i % lenUnique);
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
	public void testResizer() throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		List<ByteBuf> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 1000;
		int totalLen = 0;
		for (int i = 0; i < buffersCount; i++) {
			ByteBuf buffer = createRandomByteBuf(random);
			buffers.add(buffer);
			totalLen += buffer.remaining();
		}
		byte[] expected = byteBufsToByteArray(buffers);

		int bufSize = 128;

		StreamProducer<ByteBuf> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamByteChunker resizer = new StreamByteChunker(eventloop, bufSize / 2, bufSize);
		StreamFixedSizeConsumer streamFixedSizeConsumer = new StreamFixedSizeConsumer();

		source.streamTo(resizer);
		resizer.streamTo(streamFixedSizeConsumer);

		eventloop.run();

		List<ByteBuf> receivedBuffers = streamFixedSizeConsumer.getBuffers();
		byte[] received = byteBufsToByteArray(receivedBuffers);
		assertArrayEquals(received, expected);

		int actualLen = 0;
		for (int i = 0; i < receivedBuffers.size() - 1; i++) {
			ByteBuf buf = receivedBuffers.get(i);
			actualLen += buf.remaining();
			int receivedSize = buf.remaining();
			assertTrue(receivedSize >= bufSize / 2 && receivedSize <= bufSize);
			buf.recycle();
		}
		actualLen += receivedBuffers.get(receivedBuffers.size() - 1).remaining();
		receivedBuffers.get(receivedBuffers.size() - 1).recycle();

		assertEquals(totalLen, actualLen);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private static class StreamFixedSizeConsumer implements StreamConsumer<ByteBuf>, StreamDataReceiver<ByteBuf> {

		private List<ByteBuf> buffers = new ArrayList<>();
		private List<CompletionCallback> callbacks = new ArrayList<>();

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return this;
		}

		@Override
		public void setUpstream(StreamProducer<ByteBuf> upstreamProducer) {

		}

		@Override
		public StreamProducer<ByteBuf> getUpstream() {
			return null;
		}

		@Override
		public void onEndOfStream() {
			for (CompletionCallback callback : callbacks) {
				callback.onComplete();
			}
		}

		@Override
		public void onError(Exception e) {

		}

		@Override
		public void addCompletionCallback(CompletionCallback completionCallback) {
			callbacks.add(completionCallback);
		}

		@Override
		public void onData(ByteBuf item) {
			buffers.add(item);
		}

		public List<ByteBuf> getBuffers() {
			return buffers;
		}
	}

}