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

import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.Comparator;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * Perform a reduction on the elements of input streams using the  key function.
 * It is {@link AbstractStreamReducer}.
 *
 * @param <K> type of key for mapping
 * @param <I> type of input data
 * @param <O> type of output data
 * @param <A> type of accumulator
 */
public final class StreamReducerSimple<K, I, O, A> extends AbstractStreamReducer<K, O, A> {

	private final Function<I, K> keyFunction;
	private final Reducer<K, I, O, A> reducer;

	// region creators
	private StreamReducerSimple(Function<I, K> keyFunction, Comparator<K> keyComparator, Reducer<K, I, O, A> reducer) {
		super(keyComparator);
		this.reducer = checkNotNull(reducer);
		this.keyFunction = checkNotNull(keyFunction);
	}

	/**
	 * Creates a new instance of  StreamReducerSimple
	 *
	 * @param keyComparator comparator for compare keys
	 * @param keyFunction   function for counting key
	 */
	public static <K, I, O, A> StreamReducerSimple<K, I, O, A> create(Function<I, K> keyFunction,
			Comparator<K> keyComparator,
			Reducer<K, I, O, A> reducer) {
		return new StreamReducerSimple<>(keyFunction, keyComparator, reducer);
	}

	@Override
	public StreamReducerSimple<K, I, O, A> withBufferSize(int bufferSize) {
		return (StreamReducerSimple<K, I, O, A>) super.withBufferSize(bufferSize);
	}
	// endregion

	/**
	 * Returns  new input for this stream
	 */
	public StreamConsumer<I> newInput() {
		return newInput(keyFunction, reducer);
	}

}
