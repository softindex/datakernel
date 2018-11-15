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

package io.datakernel.stream.processor;

import io.datakernel.async.Promise;
import io.datakernel.stream.*;

/**
 * Provides to create some MapperProjection which will change received data, and send it to the
 * destination. It is {@link StreamMap} which receives original
 * data and streams changed data.
 *
 * @param <I> type of input data
 * @param <O> type of output data
 */
public final class StreamMap<I, O> implements StreamTransformer<I, O> {
	private final Input input;
	private final Output output;
	private final Mapper<I, O> mapper;

	// region creators
	private StreamMap(Mapper<I, O> mapper) {
		this.mapper = mapper;
		this.input = new Input();
		this.output = new Output();
	}

	public static <I, O> StreamMap<I, O> create(Mapper<I, O> mapper) {
		return new StreamMap<>(mapper);
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<O> getOutput() {
		return output;
	}
	// endregion

	/**
	 * Primary interface which does mapping
	 *
	 * @param <I> type of input data
	 * @param <O> type of output data
	 */
	public interface Mapper<I, O> {
		/**
		 * Holds mapping and streams it to destination
		 *
		 * @param input  received item
		 * @param output callback for streaming it to destination
		 */
		void map(I input, StreamDataAcceptor<O> output);
	}

	/**
	 * Map data with method apply and sends it to the destination
	 *
	 * @param <I> type of input data
	 * @param <O> type of output data
	 */
	public static abstract class MapperProjection<I, O> implements Mapper<I, O> {
		/**
		 * It contains operations which will be done for mapping
		 *
		 * @param input received data
		 * @return mapped object
		 */
		protected abstract O apply(I input);

		@Override
		public final void map(I input, StreamDataAcceptor<O> output) {
			O result = apply(input);
			output.accept(result);
		}
	}

	/**
	 * Filter which can map only that data which returns true in the method apply(), and sends it
	 * to the destination
	 *
	 * @param <I> type of input data
	 */
	public static abstract class MapperFilter<I> implements Mapper<I, I> {
		/**
		 * It contains some boolean expression. It resolves or does not resolves sending
		 * this data to destination
		 *
		 * @param input received data
		 * @return true, if this item must be sending, false else
		 */
		protected abstract boolean apply(I input);

		@Override
		public final void map(I input, StreamDataAcceptor<I> output) {
			if (apply(input)) {
				output.accept(input);
			}
		}
	}

	/**
	 * Provides use two mappers in one time.
	 *
	 * @param <I> type of input data for first mapper
	 * @param <T> type of output data for first mapper if type of input data for second filter
	 * @param <O> type of output data of second filter
	 * @return new mapper which is composition from two mappers from arguments
	 */
	public static <I, T, O> Mapper<I, O> combine(Mapper<I, T> mapper1, Mapper<T, O> mapper2) {
		return (input, output) -> mapper1.map(input, item -> mapper2.map(item, output));
	}

	protected final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected Promise<Void> onEndOfStream() {
			return output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable e) {
			output.close(e);
		}
	}

	protected final class Output extends AbstractStreamSupplier<O> {
		@Override
		protected void onSuspended() {
			input.getSupplier().suspend();
		}

		@Override
		protected void onError(Throwable e) {
			input.close(e);
		}

		@Override
		protected void onProduce(StreamDataAcceptor<O> dataAcceptor) {
			input.getSupplier().resume(item -> mapper.map(item, dataAcceptor));
		}
	}

	// jmx

}
