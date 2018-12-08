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
import io.datakernel.datagraph.node.NodeConsumerToList;

import java.util.List;

public final class DatasetListConsumer<T> {
	private final Object listId;

	private final Dataset<T> input;

	public DatasetListConsumer(Dataset<T> input, Object listId) {
		this.listId = listId;
		this.input = input;
	}

	public void compileInto(DataGraph graph) {
		List<StreamId> streamIds = input.channels(graph);
		for (StreamId streamId : streamIds) {
			Partition partition = graph.getPartition(streamId);
			NodeConsumerToList<T> node = new NodeConsumerToList<>(streamId, listId);
			graph.addNode(partition, node);
		}
	}
}
