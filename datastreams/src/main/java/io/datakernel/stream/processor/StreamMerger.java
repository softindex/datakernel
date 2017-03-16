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
 * Merges sorted by keys streams and streams its sorted union. It is simple
 * {@link AbstractStreamReducer} which do nothing with data and only streams it
 * sorted by keys.
 *
 * @param <K> type of key for mapping
 * @param <T> type of output data
 */
public final class StreamMerger<K, T> extends AbstractStreamReducer<K, T, Void> {

	private final Function<T, K> keyFunction;
	private final StreamReducers.Reducer<K, T, T, Void> reducer;

	// region creators
	private StreamMerger(Eventloop eventloop, Function<T, K> keyFunction, Comparator<K> keyComparator,
	                     boolean deduplicate) {
		super(eventloop, keyComparator);
		this.keyFunction = checkNotNull(keyFunction);
		this.reducer = deduplicate ? StreamReducers.<K, T>mergeDeduplicateReducer() : StreamReducers.<K, T>mergeSortReducer();
	}

	/**
	 * Returns new instance of StreamMerger
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 * @param keyFunction   function for counting key
	 * @param deduplicate   if it is true it means that in result will be not objects with same key
	 * @param <K>           type of key for mapping
	 * @param <T>           type of output data
	 */
	public static <K, T> StreamMerger<K, T> create(Eventloop eventloop, Function<T, K> keyFunction,
	                                               Comparator<K> keyComparator,
	                                               boolean deduplicate) {
		return new StreamMerger<K, T>(eventloop, keyFunction, keyComparator, deduplicate);
	}
	// endregion

	/**
	 * Adds new consumer to  StreamMerger
	 *
	 * @return this consumer
	 */
	public StreamConsumer<T> newInput() {
		return super.newInput(keyFunction, reducer);
	}

}
