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
import io.datakernel.stream.processor.StreamMerger;

import java.util.*;

/**
 * Represents a node, which merges many data streams into one, based on a logic, defined by key function and key comparator.
 *
 * @param <K> keys data type
 * @param <T> data items type
 */
public final class NodeMerge<K, T> implements Node {
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean deduplicate;

	private final List<StreamId> inputs = new ArrayList<>();
	private final StreamId output = new StreamId();

	public NodeMerge(Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate) {
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
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
		StreamMerger<K, T> streamMerger = StreamMerger.create(taskContext.getEventloop(), keyFunction, keyComparator, deduplicate);
		for (StreamId input : inputs) {
			taskContext.bindChannel(input, streamMerger.newInput());
		}
		taskContext.export(output, streamMerger.getOutput());
	}
}
