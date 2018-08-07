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
 * Provides an ability to skip certain amount of data elements while streaming.
 * Data to be skipped is determined using given {@link SkipStrategy}.
 *
 * @param <T>
 */
public final class StreamSkip<T> implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;

	private long items = 0;
	private long skip;
	private SizeCounter<T> sizeCounter;
	private Dropper<T> dropper;

	// region creators
	private StreamSkip(long skip, SizeCounter<T> sizeCounter, Dropper<T> dropper) {
		this.skip = skip;
		this.sizeCounter = sizeCounter;
		this.dropper = dropper;

		this.input = new Input();
		this.output = new Output();

	}

	public static <T> StreamSkip<T> create(long skip, SizeCounter<T> sizeCounter, Dropper<T> drop) {
		return new StreamSkip<>(skip, sizeCounter, drop);
	}

	public static <T> StreamSkip<T> create(long skip) {
		return new StreamSkip<>(skip, $ -> 1, (item, $) -> item);
	}

	public static <T> StreamSkip<T> create(long skip, SkipStrategy<T> skipStrategy) {
		return new StreamSkip<>(skip, skipStrategy.sizeCounter, skipStrategy.dropper);
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
				if (items == -1) {
					receiver.onData(item);
					return;
				}
				long size = sizeCounter.sizeOf(item);
				items += size;
				if (items >= skip) {
					long tail = (items - skip);
					items = -1;
					if (tail == 0) {
						return;
					}
					if (tail == size) {
						receiver.onData(item);
						return;
					}
					receiver.onData(dropper.drop(item, size - tail));
				}
			});
		}
	}

	/**
	 * Represents skip strategy to be used when skipping data elements.
	 *
	 * @param <T>
	 */
	public static final class SkipStrategy<T> {
		private final SizeCounter<T> sizeCounter;
		private final Dropper<T> dropper;

		public SkipStrategy(SizeCounter<T> sizeCounter, Dropper<T> dropper) {
			this.sizeCounter = sizeCounter;
			this.dropper = dropper;
		}

		public static SkipStrategy<ByteBuf> forByteBuf() {
			return new SkipStrategy<>(ByteBuf::readRemaining, (buf, skip) -> {
				ByteBuf split = buf.slice((int) skip, buf.readRemaining() - (int) skip);
				buf.recycle();
				return split;
			});
		}

		public static SkipStrategy<String> forString() {
			return new SkipStrategy<>(String::length, (str, skip) -> str.substring((int) skip));
		}
	}

	@FunctionalInterface
	public interface SizeCounter<T> {
		long sizeOf(T object);
	}

	@FunctionalInterface
	public interface Dropper<T> {
		T drop(T object, long toDrop);
	}
}
