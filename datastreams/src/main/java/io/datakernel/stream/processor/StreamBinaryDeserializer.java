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
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

/**
 * Represent deserializer which deserializes data from ByteBuffer to some type. Is a {@link AbstractStreamTransformer_1_1}
 * which receives ByteBufs and streams specified type.
 *
 * @param <T> original type of data
 */
public final class StreamBinaryDeserializer<T> extends AbstractStreamTransformer_1_1<ByteBuf, T> {
	public static final ParseException HEADER_SIZE_EXCEPTION = new ParseException("Header size is too large");
	public static final ParseException DESERIALIZED_SIZE_EXCEPTION = new ParseException("Deserialized size != parsed data size");

	private final BufferSerializer<T> valueSerializer;

	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;

	public interface Inspector extends AbstractStreamTransformer_1_1.Inspector {
		void onInput(ByteBuf buf);

		void onOutput();
	}

	public static class JmxInspector<T> extends AbstractStreamTransformer_1_1.JmxInspector implements Inspector {
		private static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;

		private final ValueStats inputBufs = ValueStats.create(SMOOTHING_WINDOW);
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
		inputConsumer = new InputConsumer();
		outputProducer = new OutputProducer(valueSerializer);
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
		private final ByteBufQueue queue = ByteBufQueue.create();

		private final BufferSerializer<T> valueSerializer;

		private final Inspector inspector = (Inspector) StreamBinaryDeserializer.this.inspector;

		private OutputProducer(BufferSerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void onData(ByteBuf buf) {
			if (inspector != null) inspector.onInput(buf);
			this.queue.add(buf);
			outputProducer.produce();
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			resumeProduce();
		}

		@Override
		protected void doProduce() {
			try {
				while (isStatusReady() && queue.hasRemaining()) {
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

					downstreamDataReceiver.onData(item);
				}

				if (isStatusReady())
					inputConsumer.resume();

				if (queue.isEmpty() && inputConsumer.getConsumerStatus().isClosed()) {
					outputProducer.sendEndOfStream();
				}

			} catch (ParseException e) {
				closeWithError(e);
			}
		}

		@Override
		protected void doCleanup() {
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