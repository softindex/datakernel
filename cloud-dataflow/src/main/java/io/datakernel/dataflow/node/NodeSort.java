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
import io.datakernel.datastream.processor.StreamSorter;
import io.datakernel.datastream.processor.StreamSorterStorage;
import io.datakernel.promise.Promise;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which performs sorting of a data stream, based on key function and key comparator.
 *
 * @param <K> keys type
 * @param <T> data items type
 */
public final class NodeSort<K, T> implements Node {

	public interface StreamSorterStorageFactory {
		<T> StreamSorterStorage<T> create(Class<T> type, TaskContext context, Promise<Void> taskExecuted);
	}

	private final Class<T> type;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean deduplicate;
	private final int itemsInMemorySize;

	private final StreamId input;
	private final StreamId output;

	public NodeSort(Class<T> type, Function<T, K> keyFunction, Comparator<K> keyComparator,
			boolean deduplicate, int itemsInMemorySize, StreamId input) {
		this(type, keyFunction, keyComparator, deduplicate, itemsInMemorySize, input, new StreamId());
	}

	public NodeSort(Class<T> type, Function<T, K> keyFunction, Comparator<K> keyComparator,
			boolean deduplicate, int itemsInMemorySize, StreamId input, StreamId output) {
		this.type = type;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
		this.itemsInMemorySize = itemsInMemorySize;
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
		StreamSorterStorageFactory storageFactory = taskContext.get(StreamSorterStorageFactory.class);
		StreamSorterStorage<T> storage = storageFactory.create(type, taskContext, taskContext.getExecutionPromise());
		StreamSorter<K, T> streamSorter = StreamSorter.create(storage, keyFunction, keyComparator, deduplicate, itemsInMemorySize);
		taskContext.bindChannel(input, streamSorter.getInput());
		taskContext.export(output, streamSorter.getOutput());
	}

	public Class<T> getType() {
		return type;
	}

	public Function<T, K> getKeyFunction() {
		return keyFunction;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public boolean isDeduplicate() {
		return deduplicate;
	}

	public int getItemsInMemorySize() {
		return itemsInMemorySize;
	}

	public StreamId getInput() {
		return input;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeSort{type=" + type +
				", keyFunction=" + keyFunction.getClass().getSimpleName() +
				", keyComparator=" + keyComparator.getClass().getSimpleName() +
				", deduplicate=" + deduplicate +
				", itemsInMemorySize=" + itemsInMemorySize +
				", input=" + input +
				", output=" + output + '}';
	}
}
