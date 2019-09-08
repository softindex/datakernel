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

package io.datakernel.dataflow.dataset.impl;

import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.dataset.LocallySortedDataset;
import io.datakernel.dataflow.graph.DataGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.datastream.processor.StreamReducers.Reducer;

import java.util.List;

import static io.datakernel.dataflow.dataset.impl.DatasetUtils.repartitionAndReduce;

public final class DatasetRepartitionReduce<K, I, O> extends Dataset<O> {
	private final LocallySortedDataset<K, I> input;
	private final Reducer<K, I, O, ?> reducer;
	private final List<Partition> partitions;

	public DatasetRepartitionReduce(LocallySortedDataset<K, I> input, Reducer<K, I, O, ?> reducer,
	                                Class<O> resultType) {
		this(input, reducer, resultType, null);
	}

	public DatasetRepartitionReduce(LocallySortedDataset<K, I> input, Reducer<K, I, O, ?> reducer,
	                                Class<O> resultType, List<Partition> partitions) {
		super(resultType);
		this.input = input;
		this.reducer = reducer;
		this.partitions = partitions;
	}

	@Override
	public List<StreamId> channels(DataGraph graph) {
		return repartitionAndReduce(graph, input, reducer,
				partitions == null || partitions.isEmpty() ? graph.getAvailablePartitions() : partitions);
	}
}
