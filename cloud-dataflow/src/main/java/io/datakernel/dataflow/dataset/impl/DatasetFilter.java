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
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public final class DatasetFilter<T> extends Dataset<T> {
	private final Dataset<T> input;
	private final Predicate<T> predicate;

	public DatasetFilter(Dataset<T> input, Predicate<T> predicate, Class<T> resultType) {
		super(resultType);
		this.input = input;
		this.predicate = predicate;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<StreamId> streamIds = input.channels(context);
		for (StreamId streamId : streamIds) {
			NodeFilter<T> node = new NodeFilter<>(predicate, streamId);
			graph.addNode(graph.getPartition(streamId), node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
