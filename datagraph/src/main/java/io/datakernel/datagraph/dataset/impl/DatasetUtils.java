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

import io.datakernel.datagraph.dataset.LocallySortedDataset;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.Partition;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.NodeDownload;
import io.datakernel.datagraph.node.NodeReduce;
import io.datakernel.datagraph.node.NodeShard;
import io.datakernel.datagraph.node.NodeUpload;
import io.datakernel.stream.processor.StreamReducers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DatasetUtils {
	private DatasetUtils() {
	}

	@SuppressWarnings("unchecked")
	public static <K, I, O> List<StreamId> repartitionAndReduce(DataGraph graph, LocallySortedDataset<K, I> input,
	                                                            StreamReducers.Reducer<K, I, O, ?> reducer,
	                                                            List<Partition> partitions) {
		Function<I, K> keyFunction = input.keyFunction();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<NodeShard<K, I>> sharders = new ArrayList<>();
		for (StreamId inputStreamId : input.channels(graph)) {
			Partition partition = graph.getPartition(inputStreamId);
			NodeShard<K, I> sharder = new NodeShard<>(keyFunction, inputStreamId);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		for (Partition partition : partitions) {
			NodeReduce<K, O, Object> streamReducer = new NodeReduce<>( // TODO
					input.keyComparator());
			graph.addNode(partition, streamReducer);

			for (NodeShard<K, I> sharder : sharders) {
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId reducerInput = forwardChannel(graph, input.valueType(), sharderOutput, partition);
				streamReducer.addInput(reducerInput, keyFunction, (StreamReducers.Reducer<K, I, O, Object>) reducer);
			}

			outputStreamIds.add(streamReducer.getOutput());
		}

		return outputStreamIds;
	}

	public static <K, T> List<StreamId> repartitionAndSort(DataGraph graph, LocallySortedDataset<K, T> input,
	                                                       List<Partition> partitions) {
		return repartitionAndReduce(graph, input, StreamReducers.<K, T>mergeSortReducer(), partitions);
	}

	public static <T> StreamId forwardChannel(DataGraph graph, Class<T> type,
	                                          StreamId sourceStreamId, Partition targetPartition) {
		Partition sourcePartition = graph.getPartition(sourceStreamId);
		return forwardChannel(graph, type, sourcePartition, targetPartition, sourceStreamId);
	}

	private static <T> StreamId forwardChannel(DataGraph graph, Class<T> type,
	                                           Partition sourcePartition, Partition targetPartition,
	                                           StreamId sourceStreamId) {
		NodeUpload<T> nodeUpload = new NodeUpload<>(type, sourceStreamId);
		NodeDownload<T> nodeDownload = new NodeDownload<>(type, sourcePartition.getAddress(), sourceStreamId);
		graph.addNode(sourcePartition, nodeUpload);
		graph.addNode(targetPartition, nodeDownload);
		return nodeDownload.getOutput();
	}
}