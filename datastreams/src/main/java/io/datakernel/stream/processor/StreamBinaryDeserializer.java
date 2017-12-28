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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.*;

import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.lang.String.format;

/**
 * Represent deserializer which deserializes data from ByteBuffer to some type. Is a {@link AbstractStreamTransformer_1_1}
 * which receives ByteBufs and streams specified type.
 *
 * @param <T> original type of data
 */
public final class StreamBinaryDeserializer<T> implements StreamTransformer<ByteBuf, T> {
	public static final ParseException HEADER_SIZE_EXCEPTION = new ParseException("Header size is too large");
	public static final ParseException DESERIALIZED_SIZE_EXCEPTION = new ParseException("Deserialized size != parsed data size");

	private final Eventloop eventloop;
	private final BufferSerializer<T> valueSerializer;

	private Input input;
	private Output output;

	// region creators
	private StreamBinaryDeserializer(Eventloop eventloop, BufferSerializer<T> valueSerializer) {
		this.eventloop = eventloop;
		this.valueSerializer = valueSerializer;
		this.input = new Input(eventloop);
		this.output = new Output(eventloop, valueSerializer);
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

	@Override
	public StreamConsumer<ByteBuf> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

	// endregion

	private final class Input extends AbstractStreamConsumer<ByteBuf> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			output.produce();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	}

	private final class Output extends AbstractStreamProducer<T> implements StreamDataReceiver<ByteBuf> {
		private final ByteBufQueue queue = ByteBufQueue.create();

		private final BufferSerializer<T> valueSerializer;

		private Output(Eventloop eventloop, BufferSerializer<T> valueSerializer) {
			super(eventloop);
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void onData(ByteBuf buf) {
			queue.add(buf);
			produce();
		}

		@Override
		protected void onWired() {
			super.onWired();
		}

		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void produce() {
			try {
				while (isReceiverReady() && queue.hasRemaining()) {
					int dataSize = tryPeekSize(queue);
					int headerSize = dataSize >>> 24;
					int size = headerSize + (dataSize & 0xFFFFFF);

					if (headerSize == 0)
						break;

					if (!queue.hasRemainingBytes(size))
						break;

					ByteBuf buf = queue.takeExactSize(size);
					buf.moveReadPosition(headerSize);

					T item;
					try {
						item = valueSerializer.deserialize(buf);
					} catch (Exception e) {
						throw new ParseException("Deserialization error", e);
					}

					if (buf.canRead())
						throw DESERIALIZED_SIZE_EXCEPTION;
					buf.recycle();

					send(item);
				}

				if (isReceiverReady()) {
					input.getProducer().produce(this);

					if (input.getStatus() == END_OF_STREAM) {
						if (queue.isEmpty()) {
							output.sendEndOfStream();
						} else {
							throw new ParseException(format("Truncated serialized data stream, %s : %s", this, queue));
						}
					}
				}
			} catch (ParseException e) {
				closeWithError(e);
			}
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}

		@Override
		protected void cleanup() {
			queue.clear();
		}
	}

	private static int tryPeekSize(ByteBufQueue queue) throws ParseException {
		assert queue.hasRemaining();
		int dataSize = 0;
		int headerSize = 0;
		byte b = queue.peekByte();
		if (b >= 0) {
			dataSize = b;
			headerSize = 1;
		} else if (queue.hasRemainingBytes(2)) {
			dataSize = b & 0x7f;
			b = queue.peekByte(1);
			if (b >= 0) {
				dataSize |= (b << 7);
				headerSize = 2;
			} else if (queue.hasRemainingBytes(3)) {
				dataSize |= ((b & 0x7f) << 7);
				b = queue.peekByte(2);
				if (b >= 0) {
					dataSize |= (b << 14);
					headerSize = 3;
				} else
					throw HEADER_SIZE_EXCEPTION;
			}
		}
		return (headerSize << 24) + dataSize;
	}

}