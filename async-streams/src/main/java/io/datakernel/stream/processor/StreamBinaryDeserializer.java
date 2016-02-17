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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializationInputBuffer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;

/**
 * Represent deserializer which deserializes data from ByteBuffer to some type. Is a {@link AbstractStreamTransformer_1_1}
 * which receives ByteBufs and streams specified type. It is one of implementation of {@link StreamDeserializer}.
 *
 * @param <T> original type of data
 */
public final class StreamBinaryDeserializer<T> extends AbstractStreamTransformer_1_1<ByteBuf, T> implements StreamDeserializer<T>, ConcurrentJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(StreamBinaryDeserializer.class);

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
		private final ArrayDeque<ByteBuf> byteBufs;
		private int bufferPos;
		public static final int MAX_HEADER_BYTES = 3;
		private final int maxMessageSize;

		public static final int INITIAL_BUFFER_SIZE = 10;

		private final BufferSerializer<T> valueSerializer;

		private final int buffersPoolSize;

		private final SerializationInputBuffer arrayInputBuffer = new SerializationInputBuffer();
		private ByteBuf buf;
		private byte[] buffer;

		private int dataSize;

		private int jmxItems;
		private int jmxBufs;
		private long jmxBytes;

		private OutputProducer(ArrayDeque<ByteBuf> byteBufs, int maxMessageSize, BufferSerializer<T> valueSerializer, int buffersPoolSize, ByteBuf buf, byte[] buffer) {
			this.byteBufs = byteBufs;
			this.maxMessageSize = maxMessageSize;
			this.valueSerializer = valueSerializer;
			this.buffersPoolSize = buffersPoolSize;
			this.buf = buf;
			this.buffer = buffer;
		}

		@Override
		public void onData(ByteBuf buf) {
			jmxBufs++;
			jmxBytes += buf.remaining();
			this.byteBufs.offer(buf);
			outputProducer.produce();
			if (this.byteBufs.size() == this.buffersPoolSize) {
				inputConsumer.suspend();
			}
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
			while (isStatusReady()) {
				ByteBuf nextBuf = byteBufs.peek();
				if (nextBuf == null)
					break;

				byte[] b = nextBuf.array();
				int off = nextBuf.position();
				int len = nextBuf.remaining();
				while (isStatusReady() && len > 0) {
					if (dataSize == 0) {
						assert bufferPos < MAX_HEADER_BYTES;
						assert buffer.length >= MAX_HEADER_BYTES;
						// read message header:
						if (bufferPos == 0 && len >= MAX_HEADER_BYTES) {
							int sizeLen = tryReadSize(b, off);
							if (sizeLen > MAX_HEADER_BYTES)
								throw new IllegalArgumentException("Parsed size length > MAX_HEADER_BYTES");
							len -= sizeLen;
							off += sizeLen;
							bufferPos = 0;
						} else {
							int readSize = min(len, MAX_HEADER_BYTES - bufferPos);
							System.arraycopy(b, off, buffer, bufferPos, readSize);
							len -= readSize;
							off += readSize;
							bufferPos += readSize;
							int sizeLen = tryReadSize(buffer, 0);
							if (sizeLen > bufferPos) {
								// Read past last position - incomplete varint in buffer, waiting for more bytes
								dataSize = 0;
								break;
							}
							int unreadSize = bufferPos - sizeLen;
							len += unreadSize;
							off -= unreadSize;
							bufferPos = 0;
						}
						if (dataSize > maxMessageSize)
							throw new IllegalArgumentException("Parsed data size > message size");
					}

					// read message body:
					T item;
					if (bufferPos == 0 && len >= dataSize) {
						arrayInputBuffer.set(b, off);
						item = valueSerializer.deserialize(arrayInputBuffer);
						if ((arrayInputBuffer.position() - off) != dataSize)
							throw new IllegalArgumentException("Deserialized size != parsed data size");
						len -= dataSize;
						off += dataSize;
						bufferPos = 0;
						dataSize = 0;
					} else {
						int readSize = min(len, dataSize - bufferPos);
						copyIntoBuffer(b, off, readSize);
						len -= readSize;
						off += readSize;
						if (bufferPos != dataSize)
							break;
						arrayInputBuffer.set(buffer, 0);
						item = valueSerializer.deserialize(arrayInputBuffer);
						if (arrayInputBuffer.position() != dataSize)
							throw new IllegalArgumentException("Deserialized size != parsed data size");
						bufferPos = 0;
						dataSize = 0;
					}
					nextBuf.position(off);
					++jmxItems;
					downstreamDataReceiver.onData(item);
				}

				if (getProducerStatus().isClosed())
					return;

				if (len != 0) {
					nextBuf.position(off);
					return;
				}

				ByteBuf poolBuffer = byteBufs.poll();
				assert poolBuffer == nextBuf;
				nextBuf.recycle();
			}

			if (byteBufs.isEmpty()) {
				if (inputConsumer.getConsumerStatus().isClosed()) {
					outputProducer.sendEndOfStream();
					logger.info("Deserialized {} objects from {}", jmxItems, inputConsumer.getUpstream());
				} else {
					if (!isStatusReady()) {
						resumeProduce();
					}
				}
			}
		}

		@Override
		protected void doCleanup() {
			recycleBufs();
		}

		private void growBuf(int newSize) {
			buf.limit(bufferPos);
			buf = ByteBufPool.resize(buf, newSize);
			buffer = buf.array();
		}

		private void copyIntoBuffer(byte[] b, int off, int len) {
			if (buffer.length < bufferPos + len) {
				growBuf(bufferPos + len);
			}
			System.arraycopy(b, off, buffer, bufferPos, len);
			bufferPos += len;
		}

		private int tryReadSize(byte[] buf, int off) {
			byte b = buf[off];
			if (b >= 0) {
				dataSize = b;
				return 1;
			}

			dataSize = b & 0x7f;
			b = buf[off + 1];
			if (b >= 0) {
				dataSize |= (b << 7);
				return 2;
			}

			dataSize |= ((b & 0x7f) << 7);
			b = buf[off + 2];
			if (b >= 0) {
				dataSize |= (b << 14);
				return 3;
			}

			dataSize = Integer.MAX_VALUE;
			return Integer.MAX_VALUE;
		}

		public void recycleBufs() {
			if (buf != null) {
				buf.recycle();
				buf = null;
				buffer = null;
			}
			for (ByteBuf byteBuf : byteBufs) {
				byteBuf.recycle();
			}
			byteBufs.clear();
		}

	}

	/**
	 * Creates a new instance of this class with default size of byte buffer pool - 16
	 *
	 * @param eventloop       event loop in which serializer will run
	 * @param valueSerializer specified BufferSerializer for this type
	 * @param maxMessageSize  maximal size of message which this deserializer can receive
	 */
	public StreamBinaryDeserializer(Eventloop eventloop, BufferSerializer<T> valueSerializer, int maxMessageSize) {
		this(eventloop, valueSerializer, maxMessageSize, 16);
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop       event loop in which serializer will run
	 * @param valueSerializer specified BufferSerializer for this type
	 * @param buffersPoolSize size of byte buffer pool, while its have not free space, consumer suspends.
	 * @param maxMessageSize  maximal size of message which this deserializer can receive
	 */
	public StreamBinaryDeserializer(Eventloop eventloop, BufferSerializer<T> valueSerializer, int maxMessageSize, int buffersPoolSize) {
		super(eventloop);
		checkArgument(maxMessageSize < (1 << (OutputProducer.MAX_HEADER_BYTES * 7)), "maxMessageSize must be less than 2 MB");
		checkArgument(buffersPoolSize > 0, "buffersPoolSize must be positive value, got %s", buffersPoolSize);

		ByteBuf buf = ByteBufPool.allocate(OutputProducer.INITIAL_BUFFER_SIZE);

		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(new ArrayDeque<ByteBuf>(buffersPoolSize),
				maxMessageSize,
				valueSerializer,
				buffersPoolSize,
				buf,
				buf.array());
	}

	@Override
	public void drainBuffersTo(StreamDataReceiver<ByteBuf> dataReceiver) {
		for (ByteBuf byteBuf : outputProducer.byteBufs) {
			dataReceiver.onData(byteBuf);
		}
		outputProducer.byteBufs.clear();
		outputProducer.sendEndOfStream();
		logger.info("Deserialized {} objects from {}", outputProducer.jmxItems, inputConsumer.getUpstream());
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