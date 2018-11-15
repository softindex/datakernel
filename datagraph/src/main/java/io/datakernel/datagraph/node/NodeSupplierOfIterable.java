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
import io.datakernel.stream.StreamSupplier;

import java.util.Collection;
import java.util.Iterator;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which produces items as an iterable stream.
 *
 * @param <T> data items type
 */
public final class NodeSupplierOfIterable<T> implements Node {
	private Object iterableId;
	private StreamId output;

	public NodeSupplierOfIterable() {
	}

	public NodeSupplierOfIterable(Object iterableId) {
		this.iterableId = iterableId;
		this.output = new StreamId();
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamSupplier<T> supplier;
		Object object = taskContext.environment().get(iterableId);
		if(object instanceof Iterator) {
			supplier = StreamSupplier.ofIterator((Iterator<T>) object);
		} else if(object instanceof Iterable) {
			supplier = StreamSupplier.ofIterable((Iterable<T>) object);
		} else
			throw new IllegalArgumentException();
		taskContext.export(output, supplier);
	}

	public Object getIterableId() {
		return iterableId;
	}

	public void setIterableId(Object iterableId) {
		this.iterableId = iterableId;
	}

	public StreamId getOutput() {
		return output;
	}

	public void setOutput(StreamId output) {
		this.output = output;
	}
}
