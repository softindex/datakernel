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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Perform a reduction on the elements of input streams using the  key function.
 * It is {@link AbstractStreamReducer}.
 *
 * @param <K> type of key for mapping
 * @param <O> type of output data
 * @param <A> type of accumulator
 * @param <I> type of input data
 */
public final class StreamReducerSimple<K, I, O, A> extends AbstractStreamReducer<K, O, A> {

	private final Function<I, K> keyFunction;
	private final StreamReducers.Reducer<K, I, O, A> reducer;

	// region creators
	private StreamReducerSimple(Eventloop eventloop, Function<I, K> keyFunction, Comparator<K> keyComparator, StreamReducers.Reducer<K, I, O, A> reducer) {
		super(eventloop, keyComparator);
		this.reducer = checkNotNull(reducer);
		this.keyFunction = checkNotNull(keyFunction);
	}

	/**
	 * Creates a new instance of  StreamReducerSimple
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 * @param keyFunction   function for counting key
	 */
	public static <K, I, O, A> StreamReducerSimple<K, I, O, A> create(Eventloop eventloop, Function<I, K> keyFunction,
	                                                                  Comparator<K> keyComparator,
	                                                                  StreamReducers.Reducer<K, I, O, A> reducer) {
		return new StreamReducerSimple<K, I, O, A>(eventloop, keyFunction, keyComparator, reducer);
	}
	// endregion

	/**
	 * Returns  new input for this stream
	 */
	public StreamConsumer<I> newInput() {
		return super.newInput(keyFunction, reducer);
	}

}
