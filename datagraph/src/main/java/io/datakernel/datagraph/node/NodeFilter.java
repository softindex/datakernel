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

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.graph.TaskContext;
import io.datakernel.stream.processor.StreamFilter;

import java.util.Arrays;
import java.util.Collection;

/**
 * Represents a node, which filters a data stream and passes to output data items which satisfy a predicate.
 *
 * @param <T> data items type
 */
public final class NodeFilter<T> implements Node {
	private final Predicate<T> predicate;
	private final StreamId input;
	private final StreamId output = new StreamId();

	public NodeFilter(Predicate<T> predicate, StreamId input) {
		this.predicate = predicate;
		this.input = input;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return Arrays.asList(output);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamFilter<T> streamFilter = StreamFilter.create(taskContext.getEventloop(), predicate);
		taskContext.bindChannel(input, streamFilter.getInput());
		taskContext.export(output, streamFilter.getOutput());
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("predicate", predicate)
				.add("input", input)
				.add("output", output)
				.toString();
	}
}
