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

import com.google.gson.Gson;
import io.datakernel.async.SimpleException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;

public final class StreamGsonDeserializer<T> extends AbstractStreamTransformer_1_1<ByteBuf, T> implements StreamDeserializer<T>, ConcurrentJmxMBean {
	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	private final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.produce();
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return outputProducer;
		}
	}

	private final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<ByteBuf> {
		private static final int INITIAL_BUFFER_SIZE = 1;

		private final BufferReader bufferReader = new BufferReader();
		private final ArrayDeque<ByteBuf> byteBufs;
		private final int buffersPoolSize;

		private final Gson gson;
		private final Class<T> type;

		private ByteBuf buf;

		private int jmxItems;
		private int jmxBufs;
		private long jmxBytes;

		private OutputProducer(ArrayDeque<ByteBuf> byteBufs, int buffersPoolSize, Gson gson, Class<T> type) {
			this.byteBufs = byteBufs;
			this.buffersPoolSize = buffersPoolSize;
			this.gson = gson;
			this.type = type;
			this.buf = ByteBufPool.allocate(INITIAL_BUFFER_SIZE);
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
			resumeProduce();
		}

		@Override
		protected void doProduce() {
			while (isStatusReady() && !byteBufs.isEmpty()) {
				ByteBuf srcBuf = byteBufs.peek();

				while (isStatusReady() && srcBuf.hasRemaining()) {
					// read message body:
					int zeroPos = findZero(srcBuf.array(), srcBuf.position(), srcBuf.remaining());

					if (zeroPos == -1) {
						buf = ByteBufPool.append(buf, srcBuf);
						break;
					}

					int readLen = zeroPos - srcBuf.position();
					if (buf.position() != 0) {
						buf = ByteBufPool.append(buf, srcBuf, readLen);
						bufferReader.set(buf.array(), 0, buf.position());
						buf.position(0);
					} else {
						bufferReader.set(srcBuf.array(), srcBuf.position(), readLen);
						srcBuf.advance(readLen);
					}
					srcBuf.advance(1);

					/*
					 *   As far as we could receive broken messages
					 *   and there is no guarantee we could handle them
					 */
					try {
						T item = gson.fromJson(bufferReader, type);
						//noinspection AssertWithSideEffects
						assert jmxItems != ++jmxItems;
						downstreamDataReceiver.onData(item);
					} catch (Exception e) {
						onError(new SimpleException("Can not serialize item of type: " + type));
					}
				}

				if (getProducerStatus().isClosed()) {
					return;
				}

				if (!srcBuf.hasRemaining()) {
					byteBufs.poll();
					srcBuf.recycle();
				}
			}

			if (byteBufs.isEmpty()) {
				if (inputConsumer.getConsumerStatus().isClosed()) {
					sendEndOfStream();
					buf.recycle();
					buf = null;
					recycleBufs();
				} else {
					inputConsumer.resume();
				}
			}
		}

		@Override
		public void onData(ByteBuf buf) {
			jmxBufs++;
			jmxBytes += buf.remaining();
			byteBufs.offer(buf);
			outputProducer.produce();
			if (byteBufs.size() == buffersPoolSize) {
				inputConsumer.suspend();
			}
		}

		private void recycleBufs() {
			bufferReader.set(null, 0, 0);
			if (buf != null) {
				buf.recycle();
				buf = null;
			}
			for (ByteBuf byteBuf : byteBufs) {
				byteBuf.recycle();
			}
			byteBufs.clear();
		}

		@Override
		protected void doCleanup() {
			recycleBufs();
		}
	}

	public StreamGsonDeserializer(Eventloop eventloop, Gson gson, Class<T> type, int buffersPoolSize) {
		super(eventloop);
		this.outputProducer = new OutputProducer(new ArrayDeque<ByteBuf>(buffersPoolSize), buffersPoolSize, gson, type);
		this.inputConsumer = new InputConsumer();
	}

	private static int findZero(byte[] b, int off, int len) {
		for (int i = off; i < off + len; i++) {
			if (b[i] == 0) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public void drainBuffersTo(StreamDataReceiver<ByteBuf> dataReceiver) {
		for (ByteBuf byteBuf : outputProducer.byteBufs) {
			dataReceiver.onData(byteBuf);
		}
		outputProducer.byteBufs.clear();
		outputProducer.sendEndOfStream();
	}

	// jmx
	@Override
	public Executor getJmxExecutor() {
		return eventloop;
	}

	@JmxAttribute
	public int getItems() {
		return outputProducer.jmxItems;
	}

	@JmxAttribute
	public int getBufs() {
		return outputProducer.jmxBufs;
	}

	@JmxAttribute
	public long getBytes() {
		return outputProducer.jmxBytes;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	@Override
	public String toString() {
		boolean assertOn = false;
		assert assertOn = true;
		return '{' + super.toString()
				+ " items:" + (assertOn ? "" + outputProducer.jmxItems : "?")
				+ " bufs:" + outputProducer.jmxBufs
				+ " bytes:" + outputProducer.jmxBytes + '}';
	}
}