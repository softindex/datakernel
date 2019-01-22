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

import io.datakernel.dataflow.dataset.LocallySortedDataset;
import io.datakernel.dataflow.graph.DataGraph;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeReduceSimple;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class DatasetLocalSortReduce<K, I, O> extends LocallySortedDataset<K, O> {
	private final LocallySortedDataset<K, I> input;
	private final Reducer<K, I, O, ?> reducer;

	public DatasetLocalSortReduce(LocallySortedDataset<K, I> input, Reducer<K, I, O, ?> reducer,
	                              Class<O> resultType, Function<O, K> resultKeyFunction) {
		super(resultType, input.keyComparator(), input.keyType(), resultKeyFunction);
		this.input = input;
		this.reducer = reducer;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<StreamId> channels(DataGraph graph) {
		List<StreamId> outputStreamIds = new ArrayList<>();
		for (StreamId streamId : input.channels(graph)) {
			NodeReduceSimple<K, I, O, Object> node = new NodeReduceSimple<>(input.keyFunction(),
					input.keyComparator(), (Reducer<K, I, O, Object>) reducer);
			node.addInput(streamId);
			graph.addNode(graph.getPartition(streamId), node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
