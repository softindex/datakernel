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
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class DatasetMap<I, O> extends Dataset<O> {
	private final Dataset<I> input;
	private final Function<I, O> mapper;

	public DatasetMap(Dataset<I> input, Function<I, O> mapper, Class<O> resultType) {
		super(resultType);
		this.input = input;
		this.mapper = mapper;
	}

	@Override
	public List<StreamId> channels(DataflowGraph graph) {
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<StreamId> streamIds = input.channels(graph);
		for (StreamId streamId : streamIds) {
			NodeMap<I, O> node = new NodeMap<>(mapper, streamId);
			graph.addNode(graph.getPartition(streamId), node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
