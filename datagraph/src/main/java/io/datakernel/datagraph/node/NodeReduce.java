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
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;

import java.util.*;

/**
 * Represents a node, which performs 'reduce' operations on a list of input streams, based on a logic, defined by key comparator, key function and reducer for each input.
 *
 * @param <K> keys type
 * @param <O> output data type
 * @param <A> accumulator type
 */
public final class NodeReduce<K, O, A> implements Node {
	private final Comparator<K> keyComparator;

	public static class Input<K, O, A> {
		private final StreamReducers.Reducer<K, ?, O, A> reducer;
		private final Function<?, K> keyFunction;

		public Input(StreamReducers.Reducer<K, ?, O, A> reducer, Function<?, K> keyFunction) {
			this.reducer = reducer;
			this.keyFunction = keyFunction;
		}
	}

	private final Map<StreamId, Input<K, O, A>> inputs = new LinkedHashMap<>();
	private final StreamId output = new StreamId();

	public NodeReduce(Comparator<K> keyComparator) {
		this.keyComparator = keyComparator;
	}

	public <I> void addInput(StreamId streamId, Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
		inputs.put(streamId, new Input<>(reducer, keyFunction));
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return Arrays.asList(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamReducer<K, O, A> streamReducer = StreamReducer.create(taskContext.getEventloop(), keyComparator, 100);
		for (StreamId streamId : inputs.keySet()) {
			Input<K, O, A> koaInput = inputs.get(streamId);
			StreamConsumer<Object> input = streamReducer.newInput(
					(Function<Object, K>) koaInput.keyFunction,
					(StreamReducers.Reducer<K, Object, O, A>) koaInput.reducer);
			taskContext.bindChannel(streamId, input);
		}
		taskContext.export(output, streamReducer.getOutput());
	}
}
