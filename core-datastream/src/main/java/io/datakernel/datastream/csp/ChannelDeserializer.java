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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.parse.TruncatedDataException;
import io.datakernel.csp.ChannelInput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.AbstractStreamSupplier;
import io.datakernel.serializer.BinarySerializer;

import static java.lang.String.format;

/**
 * Represent deserializer which deserializes data from ByteBuffer to some type. Is a stream transformer
 * which receives ByteBufs and streams specified type.
 *
 * @param <T> original type of data
 */
public final class ChannelDeserializer<T> extends AbstractStreamSupplier<T> implements WithChannelToStream<ChannelDeserializer<T>, ByteBuf, T> {
	private ChannelSupplier<ByteBuf> input;
	private final BinarySerializer<T> valueSerializer;

	private final ByteBufQueue queue = new ByteBufQueue();

	// region creators
	private ChannelDeserializer(BinarySerializer<T> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	public static <T> ChannelDeserializer<T> create(BinarySerializer<T> valueSerializer) {
		return new ChannelDeserializer<>(valueSerializer);
	}

	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			this.input = input;
			return getAcknowledgement();
		};
	}
	// endregion

	@Override
	protected void produce(AsyncProduceController async) {
		async.begin();
		ByteBuf firstBuf;
		while (isReceiverReady() && (firstBuf = queue.peekBuf()) != null) {
			int dataSize;
			int headerSize;
			int size;
			int firstBufRemaining = firstBuf.readRemaining();
			if (firstBufRemaining >= 3) {
				byte[] array = firstBuf.array();
				int pos = firstBuf.head();
				byte b = array[pos];
				if (b >= 0) {
					dataSize = b;
					headerSize = 1;
				} else {
					dataSize = b & 0x7f;
					b = array[pos + 1];
					if (b >= 0) {
						dataSize += (b << 7);
						headerSize = 2;
					} else {
						dataSize += ((b & 0x7f) << 7);
						b = array[pos + 2];
						if (b >= 0) {
							dataSize += (b << 14);
							headerSize = 3;
						} else
							throw new IllegalArgumentException("Invalid header size");
					}
				}
				size = headerSize + dataSize;

				if (firstBufRemaining >= size) {
					T item = valueSerializer.decode(array, pos + headerSize);
					send(item);
					if (firstBufRemaining != size) {
						firstBuf.moveHead(size);
					} else {
						queue.take().recycle();
					}
					continue;
				}

			} else {
				byte b = queue.peekByte();
				if (b >= 0) {
					dataSize = b;
					headerSize = 1;
				} else if (queue.hasRemainingBytes(2)) {
					dataSize = b & 0x7f;
					b = queue.peekByte(1);
					if (b >= 0) {
						dataSize += (b << 7);
						headerSize = 2;
					} else if (queue.hasRemainingBytes(3)) {
						dataSize += ((b & 0x7f) << 7);
						b = queue.peekByte(2);
						if (b >= 0) {
							dataSize += (b << 14);
							headerSize = 3;
						} else
							throw new IllegalArgumentException("Invalid header size");
					} else {
						break;
					}
				} else {
					break;
				}
				size = headerSize + dataSize;
			}

			if (!queue.hasRemainingBytes(size))
				break;

			queue.consume(size, buf -> {
				T item = valueSerializer.decode(buf.array(), buf.head() + headerSize);
				send(item);
			});
		}

		if (isReceiverReady()) {
			input.get()
					.whenResult(buf -> {
						if (buf != null) {
							queue.add(buf);
							async.resume();
						} else {
							if (queue.isEmpty()) {
								sendEndOfStream();
							} else {
								close(new TruncatedDataException(ChannelDeserializer.class, format("Truncated serialized data stream, %s : %s", this, queue)));
							}
						}
					})
					.whenException(this::close);
		} else {
			async.end();
		}
	}

	@Override
	protected void onError(Throwable e) {
		queue.recycle();
		input.close(e);
	}

}
