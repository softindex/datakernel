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
import io.datakernel.datastream.StreamConsumerToList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents a node, which saves data items in a given list.
 *
 * @param <T> data items type
 */
public final class NodeConsumerToList<T> implements Node {
	private final Object listId;
	private final StreamId input;

	/**
	 * Constructs a new node consumer, which saves data items from the given input stream to the specified list.
	 *
	 * @param input  id of input stream
	 * @param listId id of output list
	 */
	public NodeConsumerToList(StreamId input, Object listId) {
		this.listId = listId;
		this.input = input;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return Collections.emptyList();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		Object object = taskContext.environment().get(listId);
		if (object == null) {
			object = new ArrayList<>();
			taskContext.environment().with(listId, object);
		}
		StreamConsumer<T> consumer;
		if (object instanceof List) {
			consumer = StreamConsumerToList.create((List<T>) object);
		} else if (object instanceof StreamConsumer) {
			consumer = (StreamConsumer<T>) object;
		} else {
			throw new IllegalStateException("Object with id " + listId + " is not a list or stream consumer, it is " + object);
		}
		taskContext.bindChannel(input, consumer);
	}

	public Object getListId() {
		return listId;
	}

	public StreamId getInput() {
		return input;
	}

}
