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

package io.datakernel.csp.process;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataAcceptor;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.Math.max;

/**
 * Represent serializer which serializes data from some type to ByteBuffer.It is a {@link AbstractStreamTransformer}
 * which receives specified type and streams ByteBufs.
 *
 * @param <T> original type of data
 */
public final class ChannelBinarySerializer<T> extends AbstractStreamConsumer<T> implements WithStreamToChannel<ChannelBinarySerializer<T>, T, ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelBinarySerializer.class);
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException();
	public static final MemSize DEFAULT_INITIAL_BUFFER_SIZE = MemSize.kilobytes(16);

	public static final MemSize MAX_SIZE_1 = MemSize.bytes(128); // (1 << (1 * 7))
	public static final MemSize MAX_SIZE_2 = MemSize.kilobytes(16); // (1 << (2 * 7))
	public static final MemSize MAX_SIZE_3 = MemSize.megabytes(2); // (1 << (3 * 7))
	public static final MemSize MAX_SIZE = MAX_SIZE_3;

	private final BufferSerializer<T> serializer;
	private MemSize initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;
	private MemSize maxMessageSize = MAX_SIZE;
	@Nullable
	private Duration autoFlushInterval;
	private boolean skipSerializationErrors = false;

	private Input input;
	private ChannelConsumer<ByteBuf> output;

	private final ArrayDeque<ByteBuf> bufs = new ArrayDeque<>();
	private boolean flushing;

	// region creators
	private ChannelBinarySerializer(BufferSerializer<T> serializer) {
		this.serializer = serializer;
		rebuild();
	}

	private void rebuild() {
		if (input != null && input.buf != null) input.buf.recycle();
		input = new Input(serializer, initialBufferSize.toInt(), maxMessageSize.toInt(), autoFlushInterval, skipSerializationErrors);
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param serializer specified BufferSerializer for this type
	 */
	public static <T> ChannelBinarySerializer<T> create(BufferSerializer<T> serializer) {
		return new ChannelBinarySerializer<>(serializer);
	}

	public ChannelBinarySerializer<T> withInitialBufferSize(MemSize bufferSize) {
		this.initialBufferSize = bufferSize;
		rebuild();
		return this;
	}

	public ChannelBinarySerializer<T> withMaxMessageSize(MemSize maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
		rebuild();
		return this;
	}

	public ChannelBinarySerializer<T> withAutoFlushInterval(@Nullable Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		rebuild();
		return this;
	}

	public ChannelBinarySerializer<T> withSkipSerializationErrors() {
		return withSkipSerializationErrors(true);
	}

	public ChannelBinarySerializer<T> withSkipSerializationErrors(boolean skipSerializationErrors) {
		this.skipSerializationErrors = skipSerializationErrors;
		rebuild();
		return this;
	}

	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> this.output = output;
	}
	// endregion

	@Override
	protected void onStarted() {
		getSupplier().resume(input);
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		input.flush();
		return getAcknowledgement();
	}

	@Override
	protected void onError(Throwable e) {
		bufs.clear();
		if (input.buf != null) {
			input.buf.recycle();
			input.buf = ByteBuf.empty();
		}
		output.close(e);
	}

	private void doFlush() {
		if (flushing) return;
		if (!bufs.isEmpty()) {
			flushing = true;
			output.accept(bufs.poll())
					.whenComplete(($, e) -> {
						if (e == null) {
							flushing = false;
							doFlush();
						} else {
							close(e);
						}
					});
		} else {
			if (getEndOfStream().isResult()) {
				flushing = true;
				output.accept(null)
						.whenResult($ -> acknowledge());
			} else {
				getSupplier().resume(input);
			}
		}
	}

	private final class Input implements StreamDataAcceptor<T> {
		private final BufferSerializer<T> serializer;

		private ByteBuf buf = ByteBuf.empty();
		private int estimatedMessageSize;

		private final int headerSize;
		private final int maxMessageSize;
		private final int initialBufferSize;

		private final int autoFlushIntervalMillis;
		private boolean flushPosted;
		private final boolean skipSerializationErrors;

		public Input(BufferSerializer<T> serializer, int initialBufferSize, int maxMessageSize, @Nullable Duration autoFlushInterval, boolean skipSerializationErrors) {
			this.skipSerializationErrors = skipSerializationErrors;
			this.serializer = checkNotNull(serializer);
			this.maxMessageSize = maxMessageSize;
			this.headerSize = varint32Size(maxMessageSize - 1);
			this.estimatedMessageSize = 1;
			this.initialBufferSize = initialBufferSize;
			this.autoFlushIntervalMillis = autoFlushInterval == null ? -1 : (int) autoFlushInterval.toMillis();
		}

		/**
		 * After receiving data it serializes it to buffer and adds it to the outputBuffer,
		 * and flushes bytes depending on the autoFlushDelay
		 *
		 * @param item receiving item
		 */
		@Override
		public void accept(T item) {
			int positionBegin;
			int positionItem;
			for (; ; ) {
				if (buf.writeRemaining() < headerSize + estimatedMessageSize + (estimatedMessageSize >>> 2)) {
					onFullBuffer();
				}
				positionBegin = buf.writePosition();
				positionItem = positionBegin + headerSize;
				buf.writePosition(positionItem);
				try {
					serializer.serialize(buf, item);
				} catch (ArrayIndexOutOfBoundsException e) {
					onUnderEstimate(positionBegin);
					continue;
				} catch (Exception e) {
					onSerializationError(positionBegin, e);
					return;
				}
				break;
			}
			int positionEnd = buf.writePosition();
			int messageSize = positionEnd - positionItem;
			if (messageSize > estimatedMessageSize) {
				if (messageSize < maxMessageSize) {
					estimatedMessageSize = messageSize;
				} else {
					onMessageOverflow(positionBegin);
					return;
				}
			}
			writeSize(buf.array(), positionBegin, messageSize);
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

		private ByteBuf allocateBuffer() {
			return ByteBufPool.allocate(max(initialBufferSize, headerSize + estimatedMessageSize + (estimatedMessageSize >>> 2)));
		}

		private void onFullBuffer() {
			flush();
			buf = allocateBuffer();
			if (!flushPosted) {
				postFlush();
			}
		}

		private void onUnderEstimate(int positionBegin) {
			buf.writePosition(positionBegin);
			int writeRemaining = buf.writeRemaining();
			flush();
			buf = ByteBufPool.allocate(max(initialBufferSize, writeRemaining + (writeRemaining >>> 1) + 1));
		}

		private void onMessageOverflow(int positionBegin) {
			buf.writePosition(positionBegin);
			handleSerializationError(OUT_OF_BOUNDS_EXCEPTION);
		}

		private void onSerializationError(int positionBegin, Exception e) {
			buf.writePosition(positionBegin);
			handleSerializationError(e);
		}

		private void handleSerializationError(Exception e) {
			if (skipSerializationErrors) {
				logger.warn("Skipping serialization error in {}", this, e);
			} else {
				close(e);
			}
		}

		private void flush() {
			if (buf.canRead()) {
				if (!bufs.isEmpty()) {
					getSupplier().suspend();
				}
				bufs.add(buf);
				estimatedMessageSize -= estimatedMessageSize >>> 8;
			} else {
				buf.recycle();
			}
			buf = ByteBuf.empty();
			doFlush();
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
				eventloop.delayBackground(autoFlushIntervalMillis, () -> {
					flushPosted = false;
					flush();
				});
			}
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
