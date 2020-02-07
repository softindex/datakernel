/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.datastream.csp;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.datastream.AbstractStreamConsumer;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;

import static io.datakernel.common.Utils.nullify;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;
import static java.lang.Math.max;

public final class ChannelSerializer<T> extends AbstractStreamConsumer<T> implements WithStreamToChannel<ChannelSerializer<T>, T, ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelSerializer.class);
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException("Message overflow");
	public static final MemSize DEFAULT_INITIAL_BUFFER_SIZE = MemSize.kilobytes(16);

	public static final MemSize MAX_SIZE_1 = MemSize.bytes(128); // (1 << (1 * 7))
	public static final MemSize MAX_SIZE_2 = MemSize.kilobytes(16); // (1 << (2 * 7))
	public static final MemSize MAX_SIZE_3 = MemSize.megabytes(2); // (1 << (3 * 7))
	public static final MemSize MAX_SIZE = MAX_SIZE_3;

	private final BinarySerializer<T> serializer;
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
	private ChannelSerializer(BinarySerializer<T> serializer) {
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
	public static <T> ChannelSerializer<T> create(BinarySerializer<T> serializer) {
		return new ChannelSerializer<>(serializer);
	}

	public ChannelSerializer<T> withInitialBufferSize(MemSize bufferSize) {
		this.initialBufferSize = bufferSize;
		rebuild();
		return this;
	}

	public ChannelSerializer<T> withMaxMessageSize(MemSize maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
		rebuild();
		return this;
	}

	public ChannelSerializer<T> withAutoFlushInterval(@Nullable Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		rebuild();
		return this;
	}

	public ChannelSerializer<T> withSkipSerializationErrors() {
		return withSkipSerializationErrors(true);
	}

	public ChannelSerializer<T> withSkipSerializationErrors(boolean skipSerializationErrors) {
		this.skipSerializationErrors = skipSerializationErrors;
		rebuild();
		return this;
	}

	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = output;
			resume(input);
		};
	}
	// endregion

	@Override
	protected void onStarted() {
		if (output != null) {
			resume(input);
		}
	}

	@Override
	protected void onEndOfStream() {
		input.flush();
	}

	@Override
	protected void onError(Throwable e) {
		bufs.clear();
		input.buf = nullify(input.buf, ByteBuf::recycle);
		output.closeEx(e);
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
							closeEx(e);
						}
					});
		} else {
			if (isEndOfStream()) {
				flushing = true;
				output.accept(null)
						.whenResult(this::acknowledge);
			} else {
				resume(input);
			}
		}
	}

	private final class Input implements StreamDataAcceptor<T> {
		private final BinarySerializer<T> serializer;

		private ByteBuf buf = ByteBuf.empty();
		private int estimatedMessageSize;

		private final int headerSize;
		private final int maxMessageSize;
		private final int initialBufferSize;

		private final int autoFlushIntervalMillis;
		private boolean flushPosted;
		private final boolean skipSerializationErrors;

		public Input(@NotNull BinarySerializer<T> serializer, int initialBufferSize, int maxMessageSize, @Nullable Duration autoFlushInterval, boolean skipSerializationErrors) {
			this.skipSerializationErrors = skipSerializationErrors;
			this.serializer = serializer;
			this.maxMessageSize = maxMessageSize;
			this.headerSize = varintSize(maxMessageSize - 1);
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
				positionBegin = buf.tail();
				positionItem = positionBegin + headerSize;
				buf.tail(positionItem);
				try {
					buf.tail(serializer.encode(buf.array(), buf.tail(), item));
				} catch (ArrayIndexOutOfBoundsException e) {
					onUnderEstimate(positionBegin);
					continue;
				} catch (Exception e) {
					onSerializationError(positionBegin, e);
					return;
				}
				break;
			}
			int positionEnd = buf.tail();
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
			buf.tail(positionBegin);
			int writeRemaining = buf.writeRemaining();
			flush();
			buf = ByteBufPool.allocate(max(initialBufferSize, writeRemaining + (writeRemaining >>> 1) + 1));
		}

		private void onMessageOverflow(int positionBegin) {
			buf.tail(positionBegin);
			handleSerializationError(OUT_OF_BOUNDS_EXCEPTION);
		}

		private void onSerializationError(int positionBegin, Exception e) {
			buf.tail(positionBegin);
			handleSerializationError(e);
		}

		private void handleSerializationError(Exception e) {
			if (skipSerializationErrors) {
				logger.warn("Skipping serialization error in {}", this, e);
			} else {
				closeEx(e);
			}
		}

		private void flush() {
			if (buf == null) return;
			if (buf.canRead()) {
				if (!bufs.isEmpty()) {
					suspend();
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
				eventloop.postLater(wrapContext(this, () -> {
					flushPosted = false;
					flush();
				}));
			} else {
				eventloop.delayBackground(autoFlushIntervalMillis, wrapContext(this, () -> {
					flushPosted = false;
					flush();
				}));
			}
		}
	}

	private static int varintSize(int value) {
		return 1 + (31 - Integer.numberOfLeadingZeros(value)) / 7;
	}

}
