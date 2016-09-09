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

package io.datakernel.datagraph.dataset.impl;

import io.datakernel.datagraph.dataset.LocallySortedDataset;
import io.datakernel.datagraph.dataset.SortedDataset;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.Partition;
import io.datakernel.datagraph.graph.StreamId;

import java.util.List;

import static io.datakernel.datagraph.dataset.impl.DatasetUtils.repartitionAndSort;

public final class DatasetRepartitionAndSort<K, T> extends SortedDataset<K, T> {
	private final LocallySortedDataset<K, T> input;
	private final List<Partition> partitions;

	public DatasetRepartitionAndSort(LocallySortedDataset<K, T> input) {
		this(input, null);
	}

	public DatasetRepartitionAndSort(LocallySortedDataset<K, T> input, List<Partition> partitions) {
		super(input.valueType(), input.keyComparator(), input.keyType(), input.keyFunction());
		this.input = input;
		this.partitions = partitions;
	}

	@Override
	public List<StreamId> channels(DataGraph graph) {
		return repartitionAndSort(graph, input,
				partitions == null || partitions.isEmpty() ? graph.getAvailablePartitions() : partitions);
	}
}
