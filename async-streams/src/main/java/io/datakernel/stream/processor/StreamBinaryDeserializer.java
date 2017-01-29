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
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.util.MemSize;

import java.util.ArrayDeque;

import static java.lang.Math.min;

/**
 * Represent deserializer which deserializes data from ByteBuffer to some type. Is a {@link AbstractStreamTransformer_1_1}
 * which receives ByteBufs and streams specified type.
 *
 * @param <T> original type of data
 */
public final class StreamBinaryDeserializer<T> extends AbstractStreamTransformer_1_1<ByteBuf, T> {
	private final BufferSerializer<T> valueSerializer;
	private int maxMessageSize = StreamBinarySerializer.MAX_SIZE - 1;
	private int buffersToSuspend = 2;

	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;

	public interface Inspector extends AbstractStreamTransformer_1_1.Inspector {
		void onInput(ByteBuf buf);

		void onOutput();
	}

	public static class JmxInspector<T> extends AbstractStreamTransformer_1_1.JmxInspector implements Inspector {
		private final ValueStats inputBufs = ValueStats.create();
		private long outputItems;

		@Override
		public void onInput(ByteBuf buf) {
			inputBufs.recordValue(buf.readRemaining());
		}

		@Override
		public void onOutput() {
			outputItems++;
		}

		@JmxAttribute
		public ValueStats getInputBufs() {
			return inputBufs;
		}

		@JmxAttribute
		public long getOutputItems() {
			return outputItems;
		}
	}

	// region creators
	private StreamBinaryDeserializer(Eventloop eventloop, BufferSerializer<T> valueSerializer) {
		super(eventloop);
		this.valueSerializer = valueSerializer;
		rebuild();
	}

	private void rebuild() {
		if (outputProducer != null) outputProducer.buf.recycle();
		inputConsumer = new InputConsumer();
		outputProducer = new OutputProducer(new ArrayDeque<ByteBuf>(), maxMessageSize, valueSerializer, buffersToSuspend);
	}

	@Override
	protected AbstractInputConsumer getInputImpl() {
		return inputConsumer;
	}

	@Override
	protected AbstractOutputProducer getOutputImpl() {
		return outputProducer;
	}

	/**
	 * Creates a new instance of this class with default size of byte buffer pool - 16
	 *
	 * @param eventloop       event loop in which serializer will run
	 * @param valueSerializer specified BufferSerializer for this type
	 */
	public static <T> StreamBinaryDeserializer<T> create(Eventloop eventloop, BufferSerializer<T> valueSerializer) {
		return new StreamBinaryDeserializer<>(eventloop, valueSerializer);
	}

	public StreamBinaryDeserializer<T> withBuffersToSuspend(int buffersToSuspend) {
		this.buffersToSuspend = buffersToSuspend;
		rebuild();
		return this;
	}

	public StreamBinaryDeserializer<T> withMaxMessageSize(int maxMessageSize) {
		this.maxMessageSize = maxMessageSize - 1;
		rebuild();
		return this;
	}

	public StreamBinaryDeserializer<T> withMaxMessageSize(MemSize maxMessageSize) {
		return withMaxMessageSize((int) maxMessageSize.get());
	}

	public StreamBinaryDeserializer<T> withInspector(Inspector inspector) {
		super.inspector = inspector;
		rebuild();
		return this;
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

		private final int buffersToSuspend;

		private ByteBuf buf;

		private int dataSize;

		private final Inspector inspector = (Inspector) StreamBinaryDeserializer.this.inspector;

		private OutputProducer(ArrayDeque<ByteBuf> byteBufs, int maxMessageSize, BufferSerializer<T> valueSerializer,
		                       int buffersToSuspend) {
			this.byteBufs = byteBufs;
			this.maxMessageSize = maxMessageSize;
			this.valueSerializer = valueSerializer;
			this.buffersToSuspend = buffersToSuspend;
			this.buf = ByteBufPool.allocate(OutputProducer.INITIAL_BUFFER_SIZE);
		}

		@Override
		public void onData(ByteBuf buf) {
			if (inspector != null) inspector.onInput(buf);
			this.byteBufs.offer(buf);
			outputProducer.produce();
			if (this.byteBufs.size() == this.buffersToSuspend) {
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

					while (isStatusReady() && nextBuf.readRemaining() > 0) {
						if (dataSize == 0) {
							// read message header:
							if (!buf.canRead() && nextBuf.readRemaining() >= MAX_HEADER_BYTES) {
								int sizeLen = tryReadSize(nextBuf.array(), nextBuf.readPosition());
								nextBuf.moveReadPosition(sizeLen);
								if (sizeLen > MAX_HEADER_BYTES)
									throw new ParseException("Parsed size length > MAX_HEADER_BYTES");
								buf.rewind();
							} else {
								int readSize = min(nextBuf.readRemaining(), MAX_HEADER_BYTES - buf.readRemaining());
								buf = ByteBufPool.append(buf, nextBuf.array(), nextBuf.readPosition(), readSize);
								nextBuf.moveReadPosition(readSize);
								int sizeLen = tryReadSize(buf.array(), buf.readPosition());
								if (sizeLen > buf.readRemaining()) {
									// Read past last position - incomplete varint in buffer, waiting for more bytes
									dataSize = 0;
									break;
								}
								int unreadSize = buf.readRemaining() - sizeLen;
								nextBuf.moveReadPosition(-unreadSize);
								buf.rewind();
							}
							if (dataSize > maxMessageSize)
								throw new ParseException("Parsed data size > message size");
						}

						// read message body:
						T item;
						if (!buf.canRead() && nextBuf.readRemaining() >= dataSize) {
							int initialHead = nextBuf.readPosition();
							try {
								item = valueSerializer.deserialize(nextBuf);
							} catch (Exception e) {
								throw new ParseException("Cannot deserialize stream ", e);
							}
							if ((nextBuf.readPosition() - initialHead) != dataSize)
								throw new ParseException("Deserialized size != parsed data size");
							dataSize = 0;
						} else {
							int readSize = min(nextBuf.readRemaining(), dataSize - buf.readRemaining());
							buf = ByteBufPool.append(buf, nextBuf.array(), nextBuf.readPosition(), readSize);
							nextBuf.moveReadPosition(readSize);

							if (buf.readRemaining() != dataSize)
								break;

							try {
								item = valueSerializer.deserialize(buf);
							} catch (Exception e) {
								throw new ParseException("Cannot finish deserialization", e);
							}
							if (buf.canRead())
								throw new ParseException("Deserialized size != parsed data size");
							dataSize = 0;
							buf.rewind();
						}
						if (inspector != null) inspector.onOutput();
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
//						logger.info("Deserialized {} objects from {}", jmxItems, inputConsumer.getUpstream());
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
}