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
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.*;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;

/**
 * Represent serializer which serializes data from some type to ByteBuffer.It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams ByteBufs.
 *
 * @param <T> original type of data
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class StreamBinarySerializer<T> implements StreamTransformer<T, ByteBuf> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException();
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(16);

	public static final MemSize MAX_SIZE_1 = MemSize.bytes(128); // (1 << (1 * 7))
	public static final MemSize MAX_SIZE_2 = MemSize.kilobytes(16); // (1 << (2 * 7))
	public static final MemSize MAX_SIZE_3 = MemSize.megabytes(2); // (1 << (3 * 7))
	public static final MemSize MAX_SIZE = MAX_SIZE_3;

	private final Eventloop eventloop;
	private final BufferSerializer<T> serializer;
	private int defaultBufferSize = (int) DEFAULT_BUFFER_SIZE.get();
	private int maxMessageSize = (int) (MAX_SIZE.get());
	private int autoFlushIntervalMillis = -1;
	private boolean skipSerializationErrors = false;

	private Input input;
	private Output output;

	// region creators
	private StreamBinarySerializer(Eventloop eventloop, BufferSerializer<T> serializer) {
		this.eventloop = eventloop;
		this.serializer = serializer;
		rebuild();
	}

	private void rebuild() {
		if (output != null && output.outputBuf != null) output.outputBuf.recycle();
		input = new Input(eventloop);
		output = new Output(eventloop, serializer, defaultBufferSize, maxMessageSize, autoFlushIntervalMillis, skipSerializationErrors);
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop  event loop in which serializer will run
	 * @param serializer specified BufferSerializer for this type
	 */
	public static <T> StreamBinarySerializer<T> create(Eventloop eventloop, BufferSerializer<T> serializer) {
		return new StreamBinarySerializer<>(eventloop, serializer);
	}

	public StreamBinarySerializer<T> withDefaultBufferSize(int bufferSize) {
		this.defaultBufferSize = bufferSize;
		rebuild();
		return this;
	}

	public StreamBinarySerializer<T> withDefaultBufferSize(MemSize bufferSize) {
		return withDefaultBufferSize((int) bufferSize.get());
	}

	public StreamBinarySerializer<T> withMaxMessageSize(int maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
		rebuild();
		return this;
	}

	public StreamBinarySerializer<T> withMaxMessageSize(MemSize maxMessageSize) {
		return withMaxMessageSize((int) maxMessageSize.get());
	}

	public StreamBinarySerializer<T> withAutoFlush(int autoFlushIntervalMillis) {
		this.autoFlushIntervalMillis = autoFlushIntervalMillis;
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

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<ByteBuf> getOutput() {
		return output;
	}

	// endregion

	private final class Input extends AbstractStreamConsumer<T> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			output.flushAndClose();
		}

		@Override
		protected void onError(Exception e) {
			output.closeWithError(e);
		}
	}

	private final class Output extends AbstractStreamProducer<ByteBuf> implements StreamDataReceiver<T> {
		private final BufferSerializer<T> serializer;

		private final int defaultBufferSize;
		private final int maxMessageSize;
		private final int headerSize;

		private ByteBuf outputBuf = allocateBuffer();
		private int estimatedMessageSize;

		private final int autoFlushIntervalMillis;
		private boolean flushPosted;
		private final boolean skipSerializationErrors;

		public Output(Eventloop eventloop, BufferSerializer<T> serializer, int defaultBufferSize, int maxMessageSize, int autoFlushIntervalMillis, boolean skipSerializationErrors) {
			super(eventloop);
			this.skipSerializationErrors = skipSerializationErrors;
			this.serializer = checkNotNull(serializer);
			this.maxMessageSize = maxMessageSize;
			this.headerSize = varint32Size(maxMessageSize - 1);
			this.estimatedMessageSize = 1;
			this.defaultBufferSize = defaultBufferSize;
			this.autoFlushIntervalMillis = autoFlushIntervalMillis;
		}

		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void produce() {
			if (input.getStatus() != StreamStatus.END_OF_STREAM) {
				input.getProducer().produce(this);
			} else {
				flushAndClose();
			}
		}

		@Override
		protected void onError(Exception e) {
			input.closeWithError(e);
		}

		private ByteBuf allocateBuffer() {
			return ByteBufPool.allocate(max(defaultBufferSize, headerSize + estimatedMessageSize));
		}

		private void flush() {
			if (outputBuf.canRead()) {
				getLastDataReceiver().onData(outputBuf);
			} else {
				outputBuf.recycle();
			}
			outputBuf = allocateBuffer();
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
			int positionBegin;
			int positionItem;
			for (; ; ) {
				if (outputBuf.writeRemaining() < headerSize + estimatedMessageSize) {
					onFullBuffer();
				}
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
			if (messageSize >= maxMessageSize) {
				onMessageOverflow(item, positionBegin, messageSize);
				return;
			}
			writeSize(outputBuf.array(), positionBegin, messageSize);
			messageSize += messageSize >>> 2;
			if (messageSize > estimatedMessageSize)
				estimatedMessageSize = messageSize;
			else
				estimatedMessageSize -= estimatedMessageSize >>> 10;
			if (!flushPosted) {
				postFlush();
			}
		}

		private void onFullBuffer() {
			flush();
		}

		private void onSerializationError(T value, int positionBegin, Exception e) {
			outputBuf.writePosition(positionBegin);
			handleSerializationError(e);
		}

		private void onMessageOverflow(T value, int positionBegin, int messageSize) {
			outputBuf.writePosition(positionBegin);
			handleSerializationError(OUT_OF_BOUNDS_EXCEPTION);
		}

		private void onUnderEstimate(T value, int positionBegin, int positionItem) {
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
			flush();
			output.sendEndOfStream();
		}

		private void postFlush() {
			flushPosted = true;
			if (autoFlushIntervalMillis == -1)
				return;
			if (autoFlushIntervalMillis == 0) {
				eventloop.postLater(() -> {
					flushPosted = false;
					flush();
				});
			} else {
				eventloop.scheduleBackground(eventloop.currentTimeMillis() + autoFlushIntervalMillis,
						() -> {
							flushPosted = false;
							flush();
						});
			}
		}

		@Override
		protected void cleanup() {
			if (outputBuf != null) {
				outputBuf.recycle();
				outputBuf = ByteBuf.empty();
			}
		}
	}

	public void flush() {
		if (output.getStatus().isOpen() && output.getLastDataReceiver() != null) {
			output.flush();
		}
	}

	private static int varint32Size(int value) {
		if ((value & 0xffffffff << 7) == 0) return 1;
		if ((value & 0xffffffff << 14) == 0) return 2;
		if ((value & 0xffffffff << 21) == 0) return 3;
		if ((value & 0xffffffff << 28) == 0) return 4;
		return 5;
	}

}