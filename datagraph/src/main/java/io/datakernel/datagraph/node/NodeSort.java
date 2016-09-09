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
import io.datakernel.stream.processor.StreamMergeSorterStorage;
import io.datakernel.stream.processor.StreamSorter;

import java.util.Collection;
import java.util.Comparator;

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
	private final StreamId output = new StreamId();

	public NodeSort(Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate, int itemsInMemorySize,
	                StreamId input) {
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
		this.itemsInMemorySize = itemsInMemorySize;
		this.input = input;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamSorter<K, T> streamSorter = new StreamSorter<>(taskContext.getEventloop(),
				taskContext.environment().getInstance(StreamMergeSorterStorage.class),
				keyFunction, keyComparator, deduplicate, itemsInMemorySize);
		taskContext.bindChannel(input, streamSorter.getInput());
		taskContext.export(output, streamSorter.getOutput());
	}

	public StreamId getOutput() {
		return output;
	}
}
