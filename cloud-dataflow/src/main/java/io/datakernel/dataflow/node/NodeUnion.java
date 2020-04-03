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
import io.datakernel.datastream.processor.StreamUnion;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;

public final class NodeUnion<T> implements Node {
	private final List<StreamId> inputs;
	private final StreamId output;

	public NodeUnion(List<StreamId> inputs) {
		this(inputs, new StreamId());
	}

	public NodeUnion(List<StreamId> inputs, StreamId output) {
		this.inputs = inputs;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamUnion<T> streamUnion = StreamUnion.create();
		for (StreamId input : inputs) {
			taskContext.bindChannel(input, streamUnion.newInput());
		}
		taskContext.export(output, streamUnion.getOutput());
	}

	@Override
	public List<StreamId> getInputs() {
		return inputs;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeUnion{inputs=" + inputs + ", output=" + output + '}';
	}
}
