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
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;

/**
 * Represent deserializer which deserializes data from ByteBuffer to some type. Is a {@link AbstractStreamTransformer_1_1}
 * which receives ByteBufs and streams specified type.
 *
 * @param <T> original type of data
 */
public final class StreamBinaryDeserializer<T> extends AbstractStreamTransformer_1_1<ByteBuf, T> implements EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	// region creators
	private StreamBinaryDeserializer(Eventloop eventloop, BufferSerializer<T> valueSerializer,
	                                 int maxMessageSize, int buffersPoolSize) {
		super(eventloop);
		checkArgument(maxMessageSize < (1 << (OutputProducer.MAX_HEADER_BYTES * 7)), "maxMessageSize must be less than 2 MB");
		checkArgument(buffersPoolSize > 0, "buffersPoolSize must be positive value, got %s", buffersPoolSize);

		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(new ArrayDeque<ByteBuf>(buffersPoolSize), maxMessageSize,
				valueSerializer, buffersPoolSize);
	}

	/**
	 * Creates a new instance of this class with default size of byte buffer pool - 16
	 *
	 * @param eventloop       event loop in which serializer will run
	 * @param valueSerializer specified BufferSerializer for this type
	 * @param maxMessageSize  maximal size of message which this deserializer can receive
	 */
	public static <T> StreamBinaryDeserializer<T> create(Eventloop eventloop, BufferSerializer<T> valueSerializer,
	                                                     int maxMessageSize) {
		return new StreamBinaryDeserializer<>(eventloop, valueSerializer, maxMessageSize, 16);
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop       event loop in which serializer will run
	 * @param valueSerializer specified BufferSerializer for this type
	 * @param buffersPoolSize size of byte buffer pool, while its have not free space, consumer suspends.
	 * @param maxMessageSize  maximal size of message which this deserializer can receive
	 */
	public static <T> StreamBinaryDeserializer<T> create(Eventloop eventloop, BufferSerializer<T> valueSerializer,
	                                                     int maxMessageSize, int buffersPoolSize) {
		return new StreamBinaryDeserializer<>(eventloop, valueSerializer, maxMessageSize, buffersPoolSize);
	}
	// endregion

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
		public static final int MAX_HEADER_BYTES = 3;
		private final int maxMessageSize;

		public static final int INITIAL_BUFFER_SIZE = 10;

		private final BufferSerializer<T> valueSerializer;

		private final int buffersPoolSize;

		private ByteBuf buf;

		private int dataSize;

		private int jmxItems;
		private int jmxBufs;
		private long jmxBytes;

		private OutputProducer(ArrayDeque<ByteBuf> byteBufs, int maxMessageSize, BufferSerializer<T> valueSerializer,
		                       int buffersPoolSize) {
			this.byteBufs = byteBufs;
			this.maxMessageSize = maxMessageSize;
			this.valueSerializer = valueSerializer;
			this.buffersPoolSize = buffersPoolSize;
			this.buf = ByteBufPool.allocate(OutputProducer.INITIAL_BUFFER_SIZE);
		}

		@Override
		public void onData(ByteBuf buf) {
			jmxBufs++;
			jmxBytes += buf.headRemaining();
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
			try {
				while (isStatusReady()) {
					ByteBuf nextBuf = byteBufs.peek();
					if (nextBuf == null)
						break;

					while (isStatusReady() && nextBuf.headRemaining() > 0) {
						if (dataSize == 0) {
							// read message header:
							if (!buf.canRead() && nextBuf.headRemaining() >= MAX_HEADER_BYTES) {
								int sizeLen = tryReadSize(nextBuf.array(), nextBuf.head());
								nextBuf.moveHead(sizeLen);
								if (sizeLen > MAX_HEADER_BYTES)
									throw new ParseException("Parsed size length > MAX_HEADER_BYTES");
								buf.rewind();
							} else {
								int readSize = min(nextBuf.headRemaining(), MAX_HEADER_BYTES - buf.headRemaining());
								buf = ByteBufPool.append(buf, nextBuf.array(), nextBuf.head(), readSize);
								int sizeLen = tryReadSize(buf.array(), buf.head());
								if (sizeLen > buf.headRemaining()) {
									// Read past last position - incomplete varint in buffer, waiting for more bytes
									dataSize = 0;
									break;
								}
								int unreadSize = buf.headRemaining() - sizeLen;
								nextBuf.moveHead(-unreadSize);
								buf.rewind();
							}
							if (dataSize > maxMessageSize)
								throw new ParseException("Parsed data size > message size");
						}

						// read message body:
						T item;
						if (!buf.canRead() && nextBuf.headRemaining() >= dataSize) {
							int initialHead = nextBuf.head();
							try {
								item = valueSerializer.deserialize(nextBuf);
							} catch (Exception e) {
								throw new ParseException("Cannot deserialize stream ", e);
							}
							if ((nextBuf.head() - initialHead) != dataSize)
								throw new ParseException("Deserialized size != parsed data size");
							dataSize = 0;
						} else {
							int readSize = min(nextBuf.headRemaining(), dataSize - buf.headRemaining());
							buf = ByteBufPool.append(buf, nextBuf.array(), nextBuf.head(), readSize);
							nextBuf.moveHead(readSize);

							if (buf.headRemaining() != dataSize)
								break;

							try {
								item = valueSerializer.deserialize(buf);
							} catch (Exception e) {
								throw new ParseException("Cannot finish deserialization", e);
							}
							if (buf.canRead())
								throw new ParseException("Deserialized size != parsed data size");
							dataSize = 0;
						}
						++jmxItems;
						downstreamDataReceiver.onData(item);
					}

					if (getProducerStatus().isClosed())
						return;

					if (nextBuf.canRead()) {
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
			} catch (ParseException e) {
				closeWithError(e);
			}
		}

		@Override
		protected void doCleanup() {
			recycleBufs();
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
			}
			for (ByteBuf byteBuf : byteBufs) {
				byteBuf.recycle();
			}
			byteBufs.clear();
		}

	}

	// jmx
	@Override
	public Eventloop getEventloop() {
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