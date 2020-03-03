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
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeReduce;
import io.datakernel.dataflow.node.NodeReduceSimple;
import io.datakernel.dataflow.node.NodeShard;
import io.datakernel.dataflow.node.NodeSort;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public final class DatasetSplitSortReduceRepartitionReduce<K, I, O, A> extends Dataset<O> {
	private final Dataset<I> input;
	private final Function<I, K> inputKeyFunction;
	private final Function<A, K> accumulatorKeyFunction;
	private final Comparator<K> keyComparator;
	private final ReducerToResult<K, I, O, A> reducer;
	private final Class<A> accumulatorType;

	public DatasetSplitSortReduceRepartitionReduce(Dataset<I> input,
	                                               Function<I, K> inputKeyFunction,
	                                               Function<A, K> accumulatorKeyFunction,
	                                               Comparator<K> keyComparator,
	                                               ReducerToResult<K, I, O, A> reducer,
	                                               Class<O> resultType,
	                                               Class<A> accumulatorType) {
		super(resultType);
		this.input = input;
		this.inputKeyFunction = inputKeyFunction;
		this.accumulatorKeyFunction = accumulatorKeyFunction;
		this.keyComparator = keyComparator;
		this.reducer = reducer;
		this.accumulatorType = accumulatorType;
	}

	@Override
	public List<StreamId> channels(DataflowGraph graph) {
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<NodeShard<K, I>> sharders = new ArrayList<>();
		for (StreamId inputStreamId : input.channels(graph)) {
			Partition partition = graph.getPartition(inputStreamId);
			NodeShard<K, I> sharder = new NodeShard<>(inputKeyFunction, inputStreamId);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		for (Partition partition : graph.getAvailablePartitions()) {
			NodeReduce<K, O, A> nodeReduce = new NodeReduce<>(keyComparator);
			graph.addNode(partition, nodeReduce);

			for (NodeShard<K, I> sharder : sharders) {
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId reducerInput = sortReduceForward(graph, sharderOutput, partition);
				nodeReduce.addInput(reducerInput, accumulatorKeyFunction, reducer.accumulatorToOutput());
			}

			outputStreamIds.add(nodeReduce.getOutput());
		}

		return outputStreamIds;
	}

	private StreamId sortReduceForward(DataflowGraph graph, StreamId sourceStreamId, Partition targetPartition) {
		Partition sourcePartition = graph.getPartition(sourceStreamId);

		NodeSort<K, I> nodeSort = new NodeSort<>(input.valueType(), inputKeyFunction, keyComparator, false, 1_000_000, sourceStreamId);
		graph.addNode(sourcePartition, nodeSort);

		NodeReduceSimple<K, I, A, A> nodeReduce = new NodeReduceSimple<>(inputKeyFunction, keyComparator, reducer.inputToAccumulator());
		nodeReduce.addInput(nodeSort.getOutput());
		graph.addNode(sourcePartition, nodeReduce);

		return DatasetUtils.forwardChannel(graph, accumulatorType, nodeReduce.getOutput(), targetPartition);
	}
}
