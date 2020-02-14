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
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.processor.StreamReducer;
import io.datakernel.datastream.processor.StreamReducers.Reducer;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which performs 'reduce' operations on a list of input streams, based on a logic, defined by key comparator, key function and reducer for each input.
 *
 * @param <K> keys type
 * @param <O> output data type
 * @param <A> accumulator type
 */
public final class NodeReduce<K, O, A> implements Node {
	public static class Input<K, O, A> {
		private final Reducer<K, ?, O, A> reducer;
		private final Function<?, K> keyFunction;

		public Input(Reducer<K, ?, O, A> reducer, Function<?, K> keyFunction) {
			this.reducer = reducer;
			this.keyFunction = keyFunction;
		}

		public Reducer<K, ?, O, A> getReducer() {
			return reducer;
		}

		public Function<?, K> getKeyFunction() {
			return keyFunction;
		}

		@Override
		public String toString() {
			return "Input{reducer=" + reducer.getClass().getSimpleName() +
					", keyFunction=" + keyFunction.getClass().getSimpleName() + '}';
		}
	}

	private final Comparator<K> keyComparator;
	private final Map<StreamId, Input<K, O, A>> inputs;
	private final StreamId output;

	public NodeReduce(Comparator<K> keyComparator) {
		this(keyComparator, new LinkedHashMap<>(), new StreamId());
	}

	public NodeReduce(Comparator<K> keyComparator,
			Map<StreamId, Input<K, O, A>> inputs,
			StreamId output) {
		this.keyComparator = keyComparator;
		this.inputs = inputs;
		this.output = output;
	}

	public <I> void addInput(StreamId streamId, Function<I, K> keyFunction, Reducer<K, I, O, A> reducer) {
		inputs.put(streamId, new Input<>(reducer, keyFunction));
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamReducer<K, O, A> streamReducer = StreamReducer.create(keyComparator);
		for (StreamId streamId : inputs.keySet()) {
			Input<K, O, A> koaInput = inputs.get(streamId);
			StreamConsumer<Object> input = streamReducer.newInput(
					((Function<Object, K>) koaInput.keyFunction),
					(Reducer<K, Object, O, A>) koaInput.reducer);
			taskContext.bindChannel(streamId, input);
		}
		taskContext.export(output, streamReducer.getOutput());
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public Map<StreamId, Input<K, O, A>> getInputs() {
		return inputs;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeReduce{keyComparator=" + keyComparator.getClass().getSimpleName() +
				", inputs=" + inputs +
				", output=" + output + '}';
	}
}

