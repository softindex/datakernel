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
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.jmx.ValueStats;
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
public final class StreamBinarySerializer<T> extends AbstractStreamTransformer_1_1<T, ByteBuf> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException();

	public static final int MAX_SIZE_1_BYTE = 127; // (1 << (1 * 7)) - 1
	public static final int MAX_SIZE_2_BYTE = 16383; // (1 << (2 * 7)) - 1
	public static final int MAX_SIZE_3_BYTE = 2097151; // (1 << (3 * 7)) - 1
	public static final int MAX_SIZE = MAX_SIZE_3_BYTE;

	private static final int MAX_HEADER_BYTES = 3;

	private final BufferSerializer<T> serializer;
	private final int defaultBufferSize;
	private final int maxMessageSize;
	private int flushDelayMillis = 0;
	private boolean skipSerializationErrors = false;

	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;

	public interface Inspector<T> extends AbstractStreamTransformer_1_1.Inspector {
		void onUnderEstimate(T item, int estimatedMessageSize);

		void onFullBuffer();

		void onMessageOverflow(T item, int messageSize);

		void onOutput(ByteBuf buf);

		void onSerializationError(T item, Exception e);
	}

	public interface InspectorEx<T> extends Inspector<T> {
		void onItem(T item, int messageSize);
	}

	public static class JmxInspector<T> extends AbstractStreamTransformer_1_1.JmxInspector implements Inspector<T> {
		private long underEstimations;
		private long fullBuffers;
		private long messageOverflows;
		private final ValueStats outputBufs = ValueStats.create();
		private final ExceptionStats serializationErrors = ExceptionStats.create();

		@Override
		public void onUnderEstimate(T item, int estimatedMessageSize) {
			underEstimations++;
		}

		@Override
		public void onFullBuffer() {
			fullBuffers++;
		}

		@Override
		public void onMessageOverflow(T item, int messageSize) {
			messageOverflows++;
		}

		@Override
		public void onOutput(ByteBuf buf) {
			outputBufs.recordValue(buf.readRemaining());
		}

		@Override
		public void onSerializationError(T item, Exception e) {
			serializationErrors.recordException(e, item);
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getUnderEstimations() {
			return underEstimations;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getFullBuffers() {
			return fullBuffers;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getMessageOverflows() {
			return messageOverflows;
		}

		@JmxAttribute(extraSubAttributes = {"count", "max"})
		public ValueStats getOutputBufs() {
			return outputBufs;
		}

		@JmxAttribute
		public ExceptionStats getSerializationErrors() {
			return serializationErrors;
		}
	}

	public static class JmxInspectorEx<T> extends JmxInspector<T> implements InspectorEx<T> {
		private int lastSize;
		private long count;
		private final ValueStats underEstimations = ValueStats.create();
		private final EventStats fullBuffers = EventStats.create();
		private final ValueStats messageOverflows = ValueStats.create();
		private final ValueStats outputBufs = ValueStats.create();
		private final ExceptionStats serializationErrors = ExceptionStats.create();

		@Override
		public void onItem(T item, int messageSize) {
			lastSize = messageSize;
			count++;
		}

		@JmxAttribute
		public int getLastSize() {
			return lastSize;
		}

		@JmxAttribute
		public long getCount() {
			return count;
		}
	}

	// region creators
	private StreamBinarySerializer(Eventloop eventloop, BufferSerializer<T> serializer, int defaultBufferSize, int maxMessageSize) {
		super(eventloop);
		this.serializer = serializer;
		this.defaultBufferSize = defaultBufferSize;
		this.maxMessageSize = maxMessageSize;
		rebuild();
	}

	private void rebuild() {
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(serializer, defaultBufferSize, maxMessageSize, flushDelayMillis, skipSerializationErrors);
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
	 * Creates a new instance of this class
	 *
	 * @param eventloop      event loop in which serializer will run
	 * @param serializer     specified BufferSerializer for this type
	 * @param maxMessageSize maximal size of message which this serializer can receive
	 */
	public static <T> StreamBinarySerializer<T> create(Eventloop eventloop, BufferSerializer<T> serializer,
	                                                   int defaultBufferSize, int maxMessageSize) {
		return new StreamBinarySerializer<>(eventloop, serializer, defaultBufferSize, maxMessageSize);
	}

	public StreamBinarySerializer<T> withFlushDelay(int flushDelayMillis) {
		this.flushDelayMillis = flushDelayMillis;
		rebuild();
		return this;
	}

	public StreamBinarySerializer<T> withSkipSerializationErrors() {
		return withSkipSerializationErrors(true);
	}

	public StreamBinarySerializer<T> withSkipSerializationErrors(boolean skipSerializationErrors) {
		this.skipSerializationErrors = skipSerializationErrors;
		rebuild();
		return this;
	}

	public StreamBinarySerializer<T> withInspector(Inspector<T> inspector) {
		this.inspector = inspector;
		rebuild();
		return this;
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

		private final Inspector inspector = (Inspector) StreamBinarySerializer.this.inspector;
		private final InspectorEx inspectorEx = StreamBinarySerializer.this.inspector instanceof InspectorEx ?
				(InspectorEx) StreamBinarySerializer.this.inspector : null;

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
				if (inspector != null) inspector.onOutput(outputBuf);
				if (outputProducer.getProducerStatus().isOpen()) {
					receiver.onData(outputBuf);
					outputBuf = null;
				}
			} else {
				outputBuf.recycle();
			}
			allocateBuffer();
		}

		private void ensureSize(int size) {
			if (outputBuf.writeRemaining() < size) {
				if (inspector != null) inspector.onFullBuffer();
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
		 * @param item receiving item
		 */
		@Override
		public void onData(T item) {
			//noinspection AssertWithSideEffects
			int positionBegin;
			int positionItem;
			for (; ; ) {
				ensureSize(headerSize + estimatedMessageSize);
				positionBegin = outputBuf.writePosition();
				positionItem = positionBegin + headerSize;
				outputBuf.writePosition(positionItem);
				try {
					serializer.serialize(outputBuf, item);
				} catch (ArrayIndexOutOfBoundsException e) {
					onUnderEstimate(item, positionBegin, positionItem);
					continue;
				} catch (Exception e) {
					onSerializationError(item, positionBegin, e);
					return;
				}
				break;
			}
			int positionEnd = outputBuf.writePosition();
			int messageSize = positionEnd - positionItem;
			if (messageSize > maxMessageSize) {
				onMessageOverflow(item, positionBegin, messageSize);
				return;
			}
			writeSize(outputBuf.array(), positionBegin, messageSize);
			if (inspectorEx != null) inspectorEx.onItem(item, messageSize);
			messageSize += messageSize >>> 2;
			if (messageSize > estimatedMessageSize)
				estimatedMessageSize = messageSize;
			else
				estimatedMessageSize -= estimatedMessageSize >>> 8;

			if (!flushPosted) {
				postFlush();
			}
		}

		private void onSerializationError(T value, int positionBegin, Exception e) {
			if (inspector != null) inspector.onSerializationError(value, e);
			outputBuf.writePosition(positionBegin);
			handleSerializationError(e);
		}

		private void onMessageOverflow(T value, int positionBegin, int messageSize) {
			if (inspector != null) inspector.onMessageOverflow(value, messageSize);
			outputBuf.writePosition(positionBegin);
			handleSerializationError(OUT_OF_BOUNDS_EXCEPTION);
		}

		private void onUnderEstimate(T value, int positionBegin, int positionItem) {
			if (inspector != null)
				inspector.onUnderEstimate(value, estimatedMessageSize);
			outputBuf.writePosition(positionBegin);
			int messageSize = outputBuf.limit() - positionItem;
			estimatedMessageSize = messageSize + 1 + (messageSize >>> 1);
		}

		private void handleSerializationError(Exception e) {
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
				eventloop.scheduleBackground(eventloop.currentTimeMillis() + flushDelayMillis, new ScheduledRunnable() {
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
}