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

import com.google.common.base.Function;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;

import java.util.Comparator;

/**
 * Perform aggregative functions on the elements sorted by keys from input streams. Searches key of item
 * with key function, selects elements with some key, reductions it and streams result sorted by key.
 * It is {@link AbstractStreamReducer}.
 *
 * @param <K> type of key of element
 * @param <O> type of output data
 * @param <A> type of accumulator
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class StreamReducer<K, O, A> extends AbstractStreamReducer<K, O, A> {

	// region creators
	private StreamReducer(Eventloop eventloop, Comparator<K> keyComparator, int bufferSize) {
		super(eventloop, keyComparator, bufferSize);
	}

	private StreamReducer(Eventloop eventloop, Comparator<K> keyComparator) {
		super(eventloop, keyComparator);
	}

	/**
	 * Creates a new instance of StreamReducer
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 */
	public static <K, O, A> StreamReducer<K, O, A> create(Eventloop eventloop, Comparator<K> keyComparator) {
		return new StreamReducer<K, O, A>(eventloop, keyComparator);
	}

	/**
	 * Creates a new instance of StreamReducer
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 * @param bufferSize    maximal size of items which can be stored before reducing
	 */
	public static <K, O, A> StreamReducer<K, O, A> create(Eventloop eventloop, Comparator<K> keyComparator,
	                                                      int bufferSize) {
		return new StreamReducer<K, O, A>(eventloop, keyComparator, bufferSize);
	}
	// endregion

	/**
	 * Creates a new input stream for this reducer
	 *
	 * @param keyFunction function for counting key
	 * @param reducer     reducer witch will performs actions with its stream
	 * @param <I>         type of input data
	 * @return new consumer
	 */
	@Override
	public <I> StreamConsumer<I> newInput(Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
		return super.newInput(keyFunction, reducer);
	}
}
