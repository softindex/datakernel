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
import io.datakernel.stream.processor.StreamMap;

import java.util.Arrays;
import java.util.Collection;

/**
 * Represents a node, which maps input values to output values based on a logic, defined by mapper.
 *
 * @param <I> input items data type
 * @param <O> output items data type
 */
public final class NodeMap<I, O> implements Node {
	private final StreamMap.Mapper<I, O> mapper;
	private final StreamId input;
	private final StreamId output = new StreamId();

	public NodeMap(StreamMap.Mapper<I, O> mapper, StreamId input) {
		this.mapper = mapper;
		this.input = input;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return Arrays.asList(output);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamMap<I, O> streamMap = StreamMap.create(taskContext.getEventloop(), mapper);
		taskContext.bindChannel(input, streamMap.getInput());
		taskContext.export(output, streamMap.getOutput());
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeMap{mapper=" + mapper + ", input=" + input + ", output=" + output + '}';
	}
}
