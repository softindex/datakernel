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

package io.datakernel.dataflow.helper;

import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.graph.DataflowContext;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeUpload;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.common.Preconditions.checkState;

public final class PartitionedCollector<T> {
	private final Dataset<T> input;
	private final DataflowClient client;

	public PartitionedCollector(Dataset<T> input, DataflowClient client) {
		this.input = input;
		this.client = client;
	}

	public Promise<Map<Partition, List<T>>> compile(DataflowGraph graph) {
		Map<Partition, List<T>> result = new LinkedHashMap<>();

		List<Promise<Void>> streamingPromises = new ArrayList<>();
		for (StreamId streamId : input.channels(DataflowContext.of(graph))) {
			NodeUpload<String> nodeUpload = new NodeUpload<>(String.class, streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = StreamSupplier.ofPromise(client.download(partition.getAddress(), streamId, input.valueType()));
			ArrayList<T> partitionItems = new ArrayList<>();
			List<T> prev = result.put(partition, partitionItems);
			checkState(prev == null, "Partition provides multiple channels");
			streamingPromises.add(supplier.streamTo(StreamConsumerToList.create(partitionItems)));
		}
		return Promises.all(streamingPromises)
				.map($ -> result);
	}
}
