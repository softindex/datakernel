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

import com.google.common.base.Function;
import io.datakernel.datagraph.dataset.SortedDataset;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.NodeJoin;
import io.datakernel.stream.processor.StreamJoin;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.datagraph.dataset.impl.DatasetUtils.repartitionAndSort;

public final class DatasetJoin<K, L, R, V> extends SortedDataset<K, V> {
	private final SortedDataset<K, L> left;
	private final SortedDataset<K, R> right;
	private final StreamJoin.Joiner<K, L, R, V> joiner;

	public DatasetJoin(SortedDataset<K, L> left, SortedDataset<K, R> right, StreamJoin.Joiner<K, L, R, V> joiner,
	                   Class<V> resultType, Function<V, K> keyFunction) {
		super(resultType, left.keyComparator(), left.keyType(), keyFunction);
		this.left = left;
		this.right = right;
		this.joiner = joiner;
	}

	@Override
	public List<StreamId> channels(DataGraph graph) {
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<StreamId> leftStreamIds = left.channels(graph);

		List<StreamId> rightStreamIds = repartitionAndSort(graph, right, graph.getPartitions(leftStreamIds));
		assert leftStreamIds.size() == rightStreamIds.size();
		for (int i = 0; i < leftStreamIds.size(); i++) {
			StreamId leftStreamId = leftStreamIds.get(i);
			StreamId rightStreamId = rightStreamIds.get(i);
			NodeJoin<K, L, R, V> node = new NodeJoin<>(leftStreamId, rightStreamId, left.keyComparator(),
					left.keyFunction(), right.keyFunction(), joiner);
			graph.addNode(graph.getPartition(leftStreamId), node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
