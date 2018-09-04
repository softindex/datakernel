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
import io.datakernel.stream.*;


/**
 * Provides an ability to offset certain amount of data elements while streaming.
 * Data to be skipped is determined using given {@link SliceStrategy}.
 *
 * @param <T>
 */
public final class StreamCut<T> implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;
	private final long offset;
	private final long endOffset;
	private final SizeCounter<T> sizeCounter;
	private final Slicer<T> slicer;

	private long position = 0;

	// region creators
	private StreamCut(long offset, long length, SizeCounter<T> sizeCounter, Slicer<T> slicer) {
		this.offset = offset;
		this.slicer = slicer;
		this.endOffset = length == -1 ? Long.MAX_VALUE : offset + length;
		this.sizeCounter = sizeCounter;

		this.input = new Input();
		this.output = new Output();
	}

	public static <T> StreamCut<T> create(long offset, long length, SizeCounter<T> sizeCounter, Slicer<T> slicer) {
		return new StreamCut<>(offset, length, sizeCounter, slicer);
	}

	public static <T> StreamCut<T> create(long offset, long length) {
		return new StreamCut<>(offset, length, $ -> 1, (item, off, len) -> item);
	}

	public static <T> StreamCut<T> create(long offset, long size, SliceStrategy<T> sliceStrategy) {
		return new StreamCut<>(offset, size, sliceStrategy.sizeCounter, sliceStrategy.slicer);
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}
	// endregion

	protected final class Output extends AbstractStreamProducer<T> {
		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> receiver) {
			input.getProducer().produce(item -> {
				int size = sizeCounter.sizeOf(item);
				if (position + size > offset && position <= endOffset) {
					if (position < offset) {
						int cut = (int) (offset - position);
						item = slicer.slice(item, cut, size - cut);
					}
					if (position + size > endOffset) {
						item = slicer.slice(item, 0, (int) (endOffset - position));
					}
					receiver.onData(item);
				}
				position += size;
			});
		}
	}

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	}

	/**
	 * Represents offset strategy to be used when skipping data elements.
	 *
	 * @param <T>
	 */
	public static final class SliceStrategy<T> {
		private final SizeCounter<T> sizeCounter;
		private final Slicer<T> slicer;

		public SliceStrategy(SizeCounter<T> sizeCounter, Slicer<T> slicer) {
			this.sizeCounter = sizeCounter;
			this.slicer = slicer;
		}

		public static SliceStrategy<ByteBuf> forByteBuf() {
			return new SliceStrategy<>(ByteBuf::readRemaining, (buf, offset, length) -> {
				buf.moveReadPosition(offset);
				buf.moveWritePosition(length - buf.readRemaining());
				return buf;
			});
		}

		public static SliceStrategy<String> forString() {
			return new SliceStrategy<>(String::length, (str, offset, length) -> str.substring(offset, offset + length));
		}
	}

	@FunctionalInterface
	public interface SizeCounter<T> {
		int sizeOf(T object);
	}

	@FunctionalInterface
	public interface Slicer<T> {
		T slice(T object, int offset, int length);
	}
}
