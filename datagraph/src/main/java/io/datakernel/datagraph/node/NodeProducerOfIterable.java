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
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.Collection;
import java.util.Iterator;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which produces items as an iterable stream.
 *
 * @param <T> data items type
 */
public final class NodeProducerOfIterable<T> implements Node {
	private final Object iterableId;
	private final StreamId output = new StreamId();

	public NodeProducerOfIterable(Object iterableId) {
		this.iterableId = iterableId;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamProducer<T> producer;
		Object object = taskContext.environment().get(iterableId);
		if (object instanceof Iterator) {
			producer = StreamProducers.ofIterator(((Iterable<T>) object).iterator());
		} else if (object instanceof Iterable) {
			producer = StreamProducers.ofIterator(((Iterable<T>) object).iterator());
		} else
			throw new IllegalArgumentException();
		taskContext.export(output, producer);
	}

	public StreamId getOutput() {
		return output;
	}
}