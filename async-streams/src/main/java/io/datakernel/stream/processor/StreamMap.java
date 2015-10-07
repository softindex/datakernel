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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1_Stateless;
import io.datakernel.stream.StreamDataReceiver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides to create some MapperProjection which will change received data, and send it to the
 * destination. It is {@link AbstractStreamTransformer_1_1_Stateless} which receives original
 * data and streams changed data.
 *
 * @param <I> type of input data
 * @param <O> type of output data
 */
public final class StreamMap<I, O> extends AbstractStreamTransformer_1_1_Stateless<I, O> implements StreamDataReceiver<I>, StreamMapMBean {
	private int jmxItems;

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
		void map(I input, StreamDataReceiver<O> output);
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
		public final void map(I input, StreamDataReceiver<O> output) {
			O result = apply(input);
			output.onData(result);
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
		public final void map(I input, StreamDataReceiver<I> output) {
			if (apply(input)) {
				output.onData(input);
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
	public static <I, T, O> Mapper<I, O> combine(final Mapper<I, T> mapper1, final Mapper<T, O> mapper2) {
		return new Mapper<I, O>() {
			@Override
			public void map(I input, final StreamDataReceiver<O> output) {
				mapper1.map(input, new StreamDataReceiver<T>() {
					@Override
					public void onData(T item) {
						mapper2.map(item, output);
					}
				});
			}
		};
	}

	private final Mapper<I, O> mapper;

	/**
	 * Creates a new instance of stream map
	 *
	 * @param eventloop eventloop in which runs StreamMap
	 * @param mapper    mapper for applying to input data
	 */
	public StreamMap(Eventloop eventloop, Mapper<I, O> mapper) {
		super(eventloop);
		this.mapper = checkNotNull(mapper);
	}

	@Override
	protected StreamDataReceiver<I> getUpstreamDataReceiver() {
		return this;
	}

	@Override
	protected void onUpstreamEndOfStream() {
		downstreamProducer.sendEndOfStream();
		upstreamConsumer.close();
	}

	/**
	 * Changes input data and receive it to the destination
	 *
	 * @param item received data
	 */
	@Override
	public void onData(I item) {
		assert jmxItems != ++jmxItems;
		mapper.map(item, downstreamDataReceiver);
	}

	@Override
	public int getItems() {
		return jmxItems;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		String items = "?";
		assert (items = "" + jmxItems) != null;
		return '{' + super.toString() + " items:" + items + '}';
	}

	//for test only
	byte getUpstreamConsumerStatus() {
		return upstreamConsumer.getStatus();
	}

	// for test only
	byte getDownstreamProducerStatus() {
		return downstreamProducer.getStatus();
	}
}
