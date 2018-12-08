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

package io.datakernel.datagraph.node;

import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.graph.TaskContext;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.Sharders.HashSharder;
import io.datakernel.stream.processor.StreamSharder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Represents a node, which splits (duplicates) data items from a single input to many outputs.
 *
 * @param <K> keys type
 * @param <T> data items type
 */
public final class NodeShard<K, T> implements Node {
	private Function<T, K> keyFunction;

	private StreamId input;
	private List<StreamId> outputs;

	public StreamId newPartition() {
		StreamId newOutput = new StreamId();
		outputs.add(newOutput);
		return newOutput;
	}

	public StreamId getOutput(int partition) {
		return outputs.get(partition);
	}

	public NodeShard() {
	}

	public NodeShard(Function<T, K> keyFunction, StreamId input) {
		this.keyFunction = keyFunction;
		this.input = input;
		this.outputs = new ArrayList<>();
	}

	public NodeShard(Function<T, K> keyFunction, StreamId input, List<StreamId> outputs) {
		this.keyFunction = keyFunction;
		this.input = input;
		this.outputs = outputs;
	}

	public Function<T, K> getKeyFunction() {
		return keyFunction;
	}

	public void setKeyFunction(Function<T, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	public StreamId getInput() {
		return input;
	}

	public void setInput(StreamId input) {
		this.input = input;
	}

	@Override
	public List<StreamId> getOutputs() {
		return outputs;
	}

	public void setOutputs(List<StreamId> outputs) {
		this.outputs = outputs;
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		HashSharder<K> hashSharder = new HashSharder<>(outputs.size());
		StreamSharder<T> streamSharder = StreamSharder.create(
				object -> hashSharder.shard(keyFunction.apply(object)));
		taskContext.bindChannel(input, streamSharder.getInput());
		for(StreamId streamId : outputs) {
			StreamSupplier<T> supplier = streamSharder.newOutput();
			taskContext.export(streamId, supplier);
		}
	}
}
