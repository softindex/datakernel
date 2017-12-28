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

package io.datakernel.datagraph.dataset.impl;

import io.datakernel.datagraph.dataset.Dataset;
import io.datakernel.datagraph.dataset.LocallySortedDataset;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.NodeSort;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public final class DatasetLocalSort<K, T> extends LocallySortedDataset<K, T> {
	private final Dataset<T> input;

	public DatasetLocalSort(Dataset<T> input, Class<K> keyType, Function<T, K> keyFunction, Comparator<K> keyComparator) {
		super(input.valueType(), keyComparator, keyType, keyFunction);
		this.input = input;
	}

	@Override
	public List<StreamId> channels(DataGraph graph) {
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<StreamId> streamIds = input.channels(graph);
		for (StreamId streamId : streamIds) {
			NodeSort<K, T> node = new NodeSort<>(keyFunction(), keyComparator(), false, 10, streamId);
			graph.addNode(graph.getPartition(streamId), node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
