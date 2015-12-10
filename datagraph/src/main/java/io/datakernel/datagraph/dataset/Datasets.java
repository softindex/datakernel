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

package io.datakernel.datagraph.dataset;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.datakernel.datagraph.dataset.impl.*;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.stream.processor.StreamJoin;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducers;

import java.util.Comparator;
import java.util.List;

public final class Datasets {
	private Datasets() {
	}

	public static <K, T> SortedDataset<K, T> castToSorted(final Dataset<T> dataset, Class<K> keyType, Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return new SortedDataset<K, T>(dataset.valueType(), keyComparator, keyType, keyFunction) {
			@Override
			public List<StreamId> channels(DataGraph graph) {
				return dataset.channels(graph);
			}
		};
	}

	public static <K, T> SortedDataset<K, T> castToSorted(final LocallySortedDataset<K, T> dataset) {
		return new SortedDataset<K, T>(dataset.valueType(), dataset.keyComparator(), dataset.keyType(), dataset.keyFunction()) {
			@Override
			public List<StreamId> channels(DataGraph graph) {
				return dataset.channels(graph);
			}
		};
	}

	public static <K, L, R, V> Dataset<V> join(SortedDataset<K, L> left, SortedDataset<K, R> right, StreamJoin.Joiner<K, L, R, V> joiner,
	                                           Class<V> resultType) {
		return new DatasetJoin<>(left, right, joiner, resultType);
	}

	public static <I, O> Dataset<O> map(Dataset<I> dataset, StreamMap.Mapper<I, O> mapper, Class<O> resultType) {
		return new DatasetMap<>(dataset, mapper, resultType);
	}

	public static <I, O> Dataset<O> map(Dataset<I> dataset, final Function<I, O> function, Class<O> resultType) {
		return map(dataset,
				new StreamMap.MapperProjection<I, O>() {
					@Override
					protected O apply(I input) {
						return function.apply(input);
					}
				},
				resultType);
	}

	public static <T> Dataset<T> map(Dataset<T> dataset, StreamMap.Mapper<T, T> mapper) {
		return map(dataset, mapper, dataset.valueType());
	}

	public static <T> Dataset<T> map(Dataset<T> dataset, Function<T, T> function) {
		return map(dataset, function, dataset.valueType());
	}

	public static <T> Dataset<T> filter(Dataset<T> dataset, Predicate<T> predicate) {
		return new DatasetFilter<>(dataset, predicate, dataset.valueType());
	}

	public static <K, I> LocallySortedDataset<K, I> localSort(Dataset<I> dataset, Class<K> keyType, Function<I, K> keyFunction, Comparator<K> keyComparator) {
		return new DatasetLocalSort<>(dataset, keyType, keyFunction, keyComparator);
	}

	public static <K, I, O> LocallySortedDataset<K, O> localReduce(LocallySortedDataset<K, I> stream, StreamReducers.Reducer<K, I, O, ?> reducer, Class<O> resultType, Function<O, K> resultKeyFunction) {
		return new DatasetLocalSortReduce<>(stream, reducer, resultType, resultKeyFunction);
	}

	public static <K, I, O> Dataset<O> repartition_Reduce(LocallySortedDataset<K, I> dataset, StreamReducers.Reducer<K, I, O, ?> reducer, Class<O> resultType) {
		return new DatasetRepartitionReduce<>(dataset, reducer, resultType);
	}

	public static <K, T> SortedDataset<K, T> repartition_Sort(LocallySortedDataset<K, T> dataset) {
		return new DatasetRepartitionAndSort<>(dataset);
	}

	public static <K, I, O, A> Dataset<O> sort_Reduce_Repartition_Reduce(Dataset<I> dataset, StreamReducers.ReducerToResult<K, I, O, A> reducer,
	                                                                     Class<K> keyType, Function<I, K> inputKeyFunction, Comparator<K> keyComparator,
	                                                                     Class<A> accumulatorType, Function<A, K> accumulatorKeyFunction,
	                                                                     Class<O> outputType) {
		LocallySortedDataset<K, I> partiallySorted = localSort(dataset, keyType, inputKeyFunction, keyComparator);
		LocallySortedDataset<K, A> partiallyReduced = localReduce(partiallySorted, reducer.inputToAccumulator(), accumulatorType, accumulatorKeyFunction);
		Dataset<O> result = repartition_Reduce(partiallyReduced, reducer.accumulatorToOutput(), outputType);
		return result;
	}

	public static <K, I, A> Dataset<A> sort_Reduce_Repartition_Reduce(Dataset<I> dataset, StreamReducers.ReducerToResult<K, I, A, A> reducer,
	                                                                  Class<K> keyType, Function<I, K> inputKeyFunction, Comparator<K> keyComparator,
	                                                                  Class<A> accumulatorType, Function<A, K> accumulatorKeyFunction) {
		return sort_Reduce_Repartition_Reduce(dataset, reducer,
				keyType, inputKeyFunction, keyComparator,
				accumulatorType, accumulatorKeyFunction, accumulatorType
		);
	}

	public static <K, T> Dataset<T> sort_Reduce_Repartition_Reduce(Dataset<T> dataset, StreamReducers.ReducerToResult<K, T, T, T> reducer,
	                                                               Class<K> keyType, Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return sort_Reduce_Repartition_Reduce(dataset, reducer,
				keyType, keyFunction, keyComparator,
				dataset.valueType(), keyFunction, dataset.valueType()
		);
	}

	public static <T> Dataset<T> datasetOfList(Object dataId, Class<T> resultType) {
		return new DatasetListProducer<>(dataId, resultType);
	}

	public static <K, T> SortedDataset<K, T> sortedDatasetOfList(Object dataId, Class<T> resultType, Class<K> keyType, Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return castToSorted(datasetOfList(dataId, resultType), keyType, keyFunction, keyComparator);
	}

	public static <T> DatasetListConsumer<T> listConsumer(Dataset<T> input, Object listId) {
		return new DatasetListConsumer<>(input, listId);
	}

}
