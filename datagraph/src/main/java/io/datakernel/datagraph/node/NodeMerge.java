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
import io.datakernel.stream.processor.StreamMerger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which merges many data streams into one, based on a logic, defined by key function and key comparator.
 *
 * @param <K> keys data type
 * @param <T> data items type
 */
public final class NodeMerge<K, T> implements Node {
	private Function<T, K> keyFunction;
	private Comparator<K> keyComparator;
	private boolean deduplicate;
	private List<StreamId> inputs;
	private StreamId output;

	public NodeMerge() {
	}

	public NodeMerge(Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate) {
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
		this.inputs = new ArrayList<>();
		this.output = new StreamId();
	}

	public void addInput(StreamId input) {
		inputs.add(input);
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamMerger<K, T> streamMerger = StreamMerger.create(keyFunction, keyComparator, deduplicate);
		for(StreamId input : inputs) {
			taskContext.bindChannel(input, streamMerger.newInput());
		}
		taskContext.export(output, streamMerger.getOutput());
	}

	public Function<T, K> getKeyFunction() {
		return keyFunction;
	}

	public void setKeyFunction(Function<T, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public void setKeyComparator(Comparator<K> keyComparator) {
		this.keyComparator = keyComparator;
	}

	public boolean isDeduplicate() {
		return deduplicate;
	}

	public void setDeduplicate(boolean deduplicate) {
		this.deduplicate = deduplicate;
	}

	public List<StreamId> getInputs() {
		return inputs;
	}

	public void setInputs(List<StreamId> inputs) {
		this.inputs = inputs;
	}

	public StreamId getOutput() {
		return output;
	}

	public void setOutput(StreamId output) {
		this.output = output;
	}
}