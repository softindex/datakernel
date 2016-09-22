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
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.Partition;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.NodeProducerOfIterable;

import java.util.ArrayList;
import java.util.List;

public final class DatasetListProducer<T> extends Dataset<T> {
	private final Object listId;

	public DatasetListProducer(Object listId, Class<T> resultType) {
		super(resultType);
		this.listId = listId;
	}

	@Override
	public List<StreamId> channels(DataGraph graph) {
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<Partition> availablePartitions = graph.getAvailablePartitions();
		for (Partition partition : availablePartitions) {
			NodeProducerOfIterable<T> node = new NodeProducerOfIterable<>(listId);
			graph.addNode(partition, node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
