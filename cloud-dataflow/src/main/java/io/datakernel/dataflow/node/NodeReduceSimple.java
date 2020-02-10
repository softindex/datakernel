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

package io.datakernel.dataflow.node;

import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.graph.TaskContext;
import io.datakernel.datastream.processor.StreamReducerSimple;
import io.datakernel.datastream.processor.StreamReducers.Reducer;

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
	private final Function<I, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final Reducer<K, I, O, A> reducer;
	private final List<StreamId> inputs;
	private final StreamId output;

	public NodeReduceSimple(Function<I, K> keyFunction, Comparator<K> keyComparator, Reducer<K, I, O, A> reducer) {
		this(keyFunction, keyComparator, reducer, new ArrayList<>(), new StreamId());
	}

	public NodeReduceSimple(Function<I, K> keyFunction, Comparator<K> keyComparator, Reducer<K, I, O, A> reducer,
			List<StreamId> inputs, StreamId output) {
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.reducer = reducer;
		this.inputs = inputs;
		this.output = output;
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

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public Reducer<K, I, O, A> getReducer() {
		return reducer;
	}

	public List<StreamId> getInputs() {
		return inputs;
	}

	public StreamId getOutput() {
		return output;
	}
}

