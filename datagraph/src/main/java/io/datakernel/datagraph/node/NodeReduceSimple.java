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

import com.google.common.base.Function;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.graph.TaskContext;
import io.datakernel.stream.processor.StreamReducerSimple;
import io.datakernel.stream.processor.StreamReducers;

import java.util.*;

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
	private final StreamReducers.Reducer<K, I, O, A> reducer;

	private final List<StreamId> inputs = new ArrayList<>();
	private final StreamId output = new StreamId();

	public NodeReduceSimple(Function<I, K> keyFunction, Comparator<K> keyComparator, StreamReducers.Reducer<K, I, O, A> reducer) {
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.reducer = reducer;
	}

	public void addInput(StreamId input) {
		inputs.add(input);
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return Arrays.asList(output);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamReducerSimple<K, I, O, A> streamReducerSimple = StreamReducerSimple.create(taskContext.getEventloop(), keyFunction, keyComparator, reducer);
		for (StreamId input : inputs) {
			taskContext.bindChannel(input, streamReducerSimple.newInput());
		}
		taskContext.export(output, streamReducerSimple.getOutput());
	}
}
