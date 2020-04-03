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
import io.datakernel.datastream.processor.StreamMapper;

import java.util.Collection;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which maps input values to output values based on a logic, defined by mapper.
 *
 * @param <I> input items data type
 * @param <O> output items data type
 */
public final class NodeMap<I, O> implements Node {
	private final Function<I, O> function;
	private final StreamId input;
	private final StreamId output;

	public NodeMap(Function<I, O> function, StreamId input) {
		this(function, input, new StreamId());
	}

	public NodeMap(Function<I, O> function, StreamId input, StreamId output) {
		this.function = function;
		this.input = input;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return singletonList(input);
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamMapper<I, O> streamMap = StreamMapper.create(function);
		taskContext.bindChannel(input, streamMap.getInput());
		taskContext.export(output, streamMap.getOutput());
	}

	public Function<I, O> getFunction() {
		return function;
	}

	public StreamId getInput() {
		return input;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeMap{mapper=" + function.getClass().getSimpleName() + ", input=" + input + ", output=" + output + '}';
	}
}
