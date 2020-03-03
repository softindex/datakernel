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

package io.datakernel.dataflow.dataset;

import io.datakernel.dataflow.dataset.impl.*;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.datastream.processor.StreamJoin.Joiner;
import io.datakernel.datastream.processor.StreamReducers.Reducer;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public final class Datasets {

	public static <K, T> SortedDataset<K, T> castToSorted(Dataset<T> dataset, Class<K> keyType,
	                                                      Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return new SortedDataset<K, T>(dataset.valueType(), keyComparator, keyType, keyFunction) {
			@Override
			public List<StreamId> channels(DataflowGraph graph) {
				return dataset.channels(graph);
			}
		};
	}

	public static <K, T> SortedDataset<K, T> castToSorted(LocallySortedDataset<K, T> dataset) {
		return new SortedDataset<K, T>(dataset.valueType(), dataset.keyComparator(), dataset.keyType(),
				dataset.keyFunction()) {
			@Override
			public List<StreamId> channels(DataflowGraph graph) {
				return dataset.channels(graph);
			}
		};
	}

	public static <K, L, R, V> SortedDataset<K, V> join(SortedDataset<K, L> left, SortedDataset<K, R> right,
	                                                    Joiner<K, L, R, V> joiner,
	                                                    Class<V> resultType, Function<V, K> keyFunction) {
		return new DatasetJoin<>(left, right, joiner, resultType, keyFunction);
	}

	public static <I, O> Dataset<O> map(Dataset<I> dataset, Function<I, O> mapper, Class<O> resultType) {
		return new DatasetMap<>(dataset, mapper, resultType);
	}

	public static <T> Dataset<T> map(Dataset<T> dataset, Function<T, T> mapper) {
		return map(dataset, mapper, dataset.valueType());
	}

	public static <T> Dataset<T> filter(Dataset<T> dataset, Predicate<T> predicate) {
		return new DatasetFilter<>(dataset, predicate, dataset.valueType());
	}

	public static <K, I> LocallySortedDataset<K, I> localSort(Dataset<I> dataset, Class<K> keyType,
	                                                          Function<I, K> keyFunction, Comparator<K> keyComparator) {
		return new DatasetLocalSort<>(dataset, keyType, keyFunction, keyComparator);
	}

	public static <K, I, O> LocallySortedDataset<K, O> localReduce(LocallySortedDataset<K, I> stream,
	                                                               Reducer<K, I, O, ?> reducer,
	                                                               Class<O> resultType,
	                                                               Function<O, K> resultKeyFunction) {
		return new DatasetLocalSortReduce<>(stream, reducer, resultType, resultKeyFunction);
	}

	public static <K, I, O> Dataset<O> repartition_Reduce(LocallySortedDataset<K, I> dataset,
	                                                      Reducer<K, I, O, ?> reducer,
	                                                      Class<O> resultType) {
		return new DatasetRepartitionReduce<>(dataset, reducer, resultType);
	}

	public static <K, I, O> Dataset<O> repartition_Reduce(LocallySortedDataset<K, I> dataset,
	                                                      Reducer<K, I, O, ?> reducer,
	                                                      Class<O> resultType, List<Partition> partitions) {
		return new DatasetRepartitionReduce<>(dataset, reducer, resultType, partitions);
	}

	public static <K, T> SortedDataset<K, T> repartition_Sort(LocallySortedDataset<K, T> dataset) {
		return new DatasetRepartitionAndSort<>(dataset);
	}

	public static <K, T> SortedDataset<K, T> repartition_Sort(LocallySortedDataset<K, T> dataset,
	                                                          List<Partition> partitions) {
		return new DatasetRepartitionAndSort<>(dataset, partitions);
	}

	public static <K, I, O, A> Dataset<O> sort_Reduce_Repartition_Reduce(Dataset<I> dataset,
	                                                                     ReducerToResult<K, I, O, A> reducer,
	                                                                     Class<K> keyType,
	                                                                     Function<I, K> inputKeyFunction,
	                                                                     Comparator<K> keyComparator,
	                                                                     Class<A> accumulatorType,
	                                                                     Function<A, K> accumulatorKeyFunction,
	                                                                     Class<O> outputType) {
		LocallySortedDataset<K, I> partiallySorted = localSort(dataset, keyType, inputKeyFunction, keyComparator);
		LocallySortedDataset<K, A> partiallyReduced = localReduce(partiallySorted, reducer.inputToAccumulator(),
				accumulatorType, accumulatorKeyFunction);
		return repartition_Reduce(partiallyReduced, reducer.accumulatorToOutput(), outputType);
	}

	public static <K, I, A> Dataset<A> sort_Reduce_Repartition_Reduce(Dataset<I> dataset,
	                                                                  ReducerToResult<K, I, A, A> reducer,
	                                                                  Class<K> keyType,
	                                                                  Function<I, K> inputKeyFunction,
	                                                                  Comparator<K> keyComparator,
	                                                                  Class<A> accumulatorType,
	                                                                  Function<A, K> accumulatorKeyFunction) {
		return sort_Reduce_Repartition_Reduce(dataset, reducer,
				keyType, inputKeyFunction, keyComparator,
				accumulatorType, accumulatorKeyFunction, accumulatorType
		);
	}

	public static <K, T> Dataset<T> sort_Reduce_Repartition_Reduce(Dataset<T> dataset,
	                                                               ReducerToResult<K, T, T, T> reducer,
	                                                               Class<K> keyType, Function<T, K> keyFunction,
	                                                               Comparator<K> keyComparator) {
		return sort_Reduce_Repartition_Reduce(dataset, reducer,
				keyType, keyFunction, keyComparator,
				dataset.valueType(), keyFunction, dataset.valueType()
		);
	}

	public static <K, I, O, A> Dataset<O> splitSortReduce_Repartition_Reduce(Dataset<I> dataset,
	                                                                         ReducerToResult<K, I, O, A> reducer,
	                                                                         Function<I, K> inputKeyFunction,
	                                                                         Comparator<K> keyComparator,
	                                                                         Class<A> accumulatorType,
	                                                                         Function<A, K> accumulatorKeyFunction,
	                                                                         Class<O> outputType) {
		return new DatasetSplitSortReduceRepartitionReduce<>(dataset, inputKeyFunction, accumulatorKeyFunction, keyComparator,
				reducer, outputType, accumulatorType);

	}

	public static <K, I, A> Dataset<A> splitSortReduce_Repartition_Reduce(Dataset<I> dataset,
	                                                                      ReducerToResult<K, I, A, A> reducer,
	                                                                      Function<I, K> inputKeyFunction,
	                                                                      Comparator<K> keyComparator,
	                                                                      Class<A> accumulatorType,
	                                                                      Function<A, K> accumulatorKeyFunction) {
		return splitSortReduce_Repartition_Reduce(dataset, reducer,
				inputKeyFunction, keyComparator,
				accumulatorType, accumulatorKeyFunction, accumulatorType
		);
	}

	public static <K, T> Dataset<T> splitSortReduce_Repartition_Reduce(Dataset<T> dataset,
	                                                                   ReducerToResult<K, T, T, T> reducer,
	                                                                   Function<T, K> keyFunction,
	                                                                   Comparator<K> keyComparator) {
		return splitSortReduce_Repartition_Reduce(dataset, reducer,
				keyFunction, keyComparator,
				dataset.valueType(), keyFunction, dataset.valueType()
		);
	}

	public static <T> Dataset<T> datasetOfList(Object dataId, Class<T> resultType) {
		return new DatasetListSupplier<>(dataId, resultType);
	}

	public static <K, T> SortedDataset<K, T> sortedDatasetOfList(Object dataId, Class<T> resultType, Class<K> keyType,
	                                                             Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return castToSorted(datasetOfList(dataId, resultType), keyType, keyFunction, keyComparator);
	}

	public static <T> DatasetListConsumer<T> listConsumer(Dataset<T> input, Object listId) {
		return new DatasetListConsumer<>(input, listId);
	}
}
