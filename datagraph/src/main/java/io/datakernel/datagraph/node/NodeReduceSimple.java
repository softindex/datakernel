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
import io.datakernel.stream.processor.StreamReducerSimple;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * Represents a simple reducer node, with a single key function and reducer for all internalConsumers.
 *
 * @param <K> keys type
 * @param <I> input data type
 * @param <O> output data type
 * @param <A> accumulator type
 */
public final class NodeReduceSimple<K, I, O, A> implements Node {
	private Function<I, K> keyFunction;
	private Comparator<K> keyComparator;
	private Reducer<K, I, O, A> reducer;
	private List<StreamId> inputs;
	private StreamId output;

	public NodeReduceSimple() {
	}

	public NodeReduceSimple(Function<I, K> keyFunction, Comparator<K> keyComparator, Reducer<K, I, O, A> reducer) {
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.reducer = reducer;
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
		StreamReducerSimple<K, I, O, A> streamReducerSimple =
				StreamReducerSimple.create(keyFunction, keyComparator, reducer);
		for (StreamId input : inputs) {
			taskContext.bindChannel(input, streamReducerSimple.newInput());
		}
		taskContext.export(output, streamReducerSimple.getOutput());
	}

	public Function<I, K> getKeyFunction() {
		return keyFunction;
	}

	public void setKeyFunction(Function<I, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public void setKeyComparator(Comparator<K> keyComparator) {
		this.keyComparator = keyComparator;
	}

	public Reducer<K, I, O, A> getReducer() {
		return reducer;
	}

	public void setReducer(Reducer<K, I, O, A> reducer) {
		this.reducer = reducer;
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

