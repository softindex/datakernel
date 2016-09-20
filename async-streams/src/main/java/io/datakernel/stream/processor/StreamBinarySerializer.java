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
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;

/**
 * Represent serializer which serializes data from some type to ByteBuffer.It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams ByteBufs.
 *
 * @param <T> original type of data
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class StreamBinarySerializer<T> extends AbstractStreamTransformer_1_1<T, ByteBuf> implements EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException();

	public static final int MAX_SIZE_1_BYTE = 127; // (1 << (1 * 7)) - 1
	public static final int MAX_SIZE_2_BYTE = 16383; // (1 << (2 * 7)) - 1
	public static final int MAX_SIZE_3_BYTE = 2097151; // (1 << (3 * 7)) - 1
	public static final int MAX_SIZE = MAX_SIZE_3_BYTE;

	private static final int MAX_HEADER_BYTES = 3;

	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	// region creators
	private StreamBinarySerializer(Eventloop eventloop, BufferSerializer<T> serializer, int defaultBufferSize, int maxMessageSize, int flushDelayMillis, boolean skipSerializationErrors) {
		super(eventloop);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(serializer, defaultBufferSize, maxMessageSize, flushDelayMillis, skipSerializationErrors);
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop      event loop in which serializer will run
	 * @param serializer     specified BufferSerializer for this type
	 * @param maxMessageSize maximal size of message which this serializer can receive
	 */
	public static <T> StreamBinarySerializer<T> create(Eventloop eventloop, BufferSerializer<T> serializer,
	                                                   int defaultBufferSize, int maxMessageSize,
	                                                   int flushDelayMillis, boolean skipSerializationErrors) {
		return new StreamBinarySerializer<>(eventloop, serializer, defaultBufferSize,
				maxMessageSize, flushDelayMillis, skipSerializationErrors);
	}
	// endregion

	private final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.flushAndClose();
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return outputProducer;
		}
	}

	private final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<T> {
		private final BufferSerializer<T> serializer;

		private final int defaultBufferSize;
		private final int maxMessageSize;
		private final int headerSize;

		// TODO (dvolvach): queue of serialized buffers
		private ByteBuf outputBuf;
		private int estimatedMessageSize;

		private final int flushDelayMillis;
		private boolean flushPosted;

		private final boolean skipSerializationErrors;
		private int serializationErrors;

		private int jmxItems;
		private long jmxBytes;
		private int jmxBufs;

		public OutputProducer(BufferSerializer<T> serializer, int defaultBufferSize, int maxMessageSize, int flushDelayMillis, boolean skipSerializationErrors) {
			checkArgument(maxMessageSize > 0 && maxMessageSize <= MAX_SIZE, "maxMessageSize must be in [4B..2MB) range, got %s", maxMessageSize);
			checkArgument(defaultBufferSize > 0, "defaultBufferSize must be positive value, got %s", defaultBufferSize);

			this.skipSerializationErrors = skipSerializationErrors;
			this.serializer = checkNotNull(serializer);
			this.maxMessageSize = maxMessageSize;
			this.headerSize = varint32Size(maxMessageSize);
			this.estimatedMessageSize = 1;
			this.defaultBufferSize = defaultBufferSize;
			this.flushDelayMillis = flushDelayMillis;
			allocateBuffer();
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

		private int varint32Size(int value) {
			if ((value & 0xffffffff << 7) == 0) return 1;
			if ((value & 0xffffffff << 14) == 0) return 2;
			if ((value & 0xffffffff << 21) == 0) return 3;
			if ((value & 0xffffffff << 28) == 0) return 4;
			return 5;
		}

		private void allocateBuffer() {
			outputBuf = ByteBufPool.allocate(max(defaultBufferSize, headerSize + estimatedMessageSize));
		}

		private void flushBuffer(StreamDataReceiver<ByteBuf> receiver) {
			if (outputBuf.canRead()) {
				jmxBytes += outputBuf.headRemaining();
				jmxBufs++;
				if (outputProducer.getProducerStatus().isOpen()) {
					receiver.onData(outputBuf);
				}
			} else {
				outputBuf.recycle();
			}
			allocateBuffer();
		}

		private void ensureSize(int size) {
			if (outputBuf.tailRemaining() < size) {
				flushBuffer(outputProducer.getDownstreamDataReceiver());
			}
		}

		private void writeSize(byte[] buf, int pos, int size) {
			if (headerSize == 1) {
				buf[pos] = (byte) size;
				return;
			}

			buf[pos] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			if (headerSize == 2) {
				buf[pos + 1] = (byte) size;
				return;
			}

			assert headerSize == 3;

			buf[pos + 1] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			buf[pos + 2] = (byte) size;
		}

		/**
		 * After receiving data it serializes it to buffer and adds it to the outputBuffer,
		 * and flushes bytes depending on the autoFlushDelay
		 *
		 * @param value receiving item
		 */
		@Override
		public void onData(T value) {
			//noinspection AssertWithSideEffects
			assert jmxItems != ++jmxItems;
			int positionBegin;
			int positionItem;
			for (; ; ) {
				ensureSize(headerSize + estimatedMessageSize);
				positionBegin = outputBuf.tail();
				positionItem = positionBegin + headerSize;
				outputBuf.tail(positionItem);
				try {
					serializer.serialize(outputBuf, value);
				} catch (ArrayIndexOutOfBoundsException e) {
					outputBuf.tail(positionBegin);
					int messageSize = outputBuf.limit() - positionItem;
					estimatedMessageSize = messageSize + 1 + (messageSize >>> 1);
					continue;
				} catch (Exception e) {
					outputBuf.tail(positionBegin);
					handleSerializationError(e);
					return;
				}
				break;
			}
			int positionEnd = outputBuf.tail();
			int messageSize = positionEnd - positionItem;
			if (messageSize > maxMessageSize) {
				outputBuf.tail(positionBegin);
				handleSerializationError(OUT_OF_BOUNDS_EXCEPTION);
				return;
			}
			writeSize(outputBuf.array(), positionBegin, messageSize);
			messageSize += messageSize >>> 2;
			if (messageSize > estimatedMessageSize)
				estimatedMessageSize = messageSize;
			else
				estimatedMessageSize -= estimatedMessageSize >>> 8;

			if (!flushPosted) {
				postFlush();
			}
		}

		private void handleSerializationError(Exception e) {
			serializationErrors++;
			if (skipSerializationErrors) {
				logger.warn("Skipping serialization error in {}", this, e);
			} else {
				closeWithError(e);
			}
		}

		private void flushAndClose() {
			flushBuffer(outputProducer.getDownstreamDataReceiver());
			outputBuf.recycle();
			outputBuf = null;
			logger.trace("endOfStream {}, upstream: {}", this, inputConsumer.getUpstream());
			outputProducer.sendEndOfStream();
		}

		/**
		 * Bytes will be sent immediately.
		 */
		private void flush() {
			flushBuffer(outputProducer.getDownstreamDataReceiver());
			flushPosted = false;
		}

		private void postFlush() {
			flushPosted = true;
			if (flushDelayMillis == 0) {
				eventloop.postLater(new Runnable() {
					@Override
					public void run() {
						if (outputProducer.getProducerStatus().isOpen()) {
							flush();
						}
					}
				});
			} else {
				eventloop.scheduleBackground(eventloop.currentTimeMillis() + flushDelayMillis, new Runnable() {
					@Override
					public void run() {
						if (outputProducer.getProducerStatus().isOpen()) {
							flush();
						}
					}
				});
			}
		}

		@Override
		protected void doCleanup() {
			if (outputBuf != null) {
				outputBuf.recycle();
				outputBuf = null;
			}
		}
	}

	public void flush() {
		outputProducer.flush();
	}

	// JMX
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

	@SuppressWarnings("AssertWithSideEffects")
	@JmxAttribute
	public int getSerializationErrors() {
		return outputProducer.serializationErrors;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	@Override
	public String toString() {
		boolean assertOn = false;
		assert assertOn = true;
		return '{' + super.toString()
				+ (outputProducer.serializationErrors != 0 ? "serializationErrors:" + outputProducer.serializationErrors : "")
				+ " items:" + (assertOn ? "" + outputProducer.jmxItems : "?")
				+ " bufs:" + outputProducer.jmxBufs
				+ " bytes:" + outputProducer.jmxBytes + '}';
	}
}