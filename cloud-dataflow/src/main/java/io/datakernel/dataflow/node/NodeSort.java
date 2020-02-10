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

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which performs sorting of a data stream, based on key function and key comparator.
 *
 * @param <K> keyz type
 * @param <T> data items type
 */
public final class NodeSort<K, T> implements Node {
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean deduplicate;
	private final int itemsInMemorySize;

	private final StreamId input;
	private final StreamId output;

	public NodeSort(Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate, int itemsInMemorySize, StreamId input) {
		this(keyFunction, keyComparator, deduplicate, itemsInMemorySize, input, new StreamId());
	}

	public NodeSort(Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate, int itemsInMemorySize, StreamId input, StreamId output) {
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
		this.itemsInMemorySize = itemsInMemorySize;
		this.input = input;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamSorter<K, T> streamSorter = StreamSorter.create(
				taskContext.environment().getInstance(StreamSorterStorage.class),
				keyFunction, keyComparator, deduplicate, itemsInMemorySize);
		taskContext.bindChannel(input, streamSorter.getInput());
		taskContext.export(output, streamSorter.getOutput());
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
}
