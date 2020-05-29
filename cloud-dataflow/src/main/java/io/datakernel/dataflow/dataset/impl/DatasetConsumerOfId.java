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
import io.datakernel.dataflow.graph.DataflowContext;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeConsumerOfId;

import java.util.Collections;
import java.util.List;

public final class DatasetConsumerOfId<T> extends Dataset<T> {
	private final String id;

	private final Dataset<T> input;

	public DatasetConsumerOfId(Dataset<T> input, String id) {
		super(input.valueType());
		this.id = id;
		this.input = input;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> streamIds = input.channels(context);
		for (int i = 0, streamIdsSize = streamIds.size(); i < streamIdsSize; i++) {
			StreamId streamId = streamIds.get(i);
			Partition partition = graph.getPartition(streamId);
			NodeConsumerOfId<T> node = new NodeConsumerOfId<>(id, i, streamIdsSize, streamId);
			graph.addNode(partition, node);
		}
		return Collections.emptyList();
	}
}
