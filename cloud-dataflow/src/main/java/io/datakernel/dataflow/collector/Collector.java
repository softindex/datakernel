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

package io.datakernel.dataflow.collector;

import io.datakernel.dataflow.DataflowClient;
import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.graph.DataflowContext;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeUpload;
import io.datakernel.datastream.StreamSupplier;

import java.util.ArrayList;
import java.util.List;

public final class Collector<T> {
	private final Dataset<T> input;
	private final DataflowClient client;

	public Collector(Dataset<T> input, DataflowClient client) {
		this.input = input;
		this.client = client;
	}

	public StreamSupplier<T> compile(DataflowGraph graph) {
		List<StreamId> inputStreamIds = input.channels(DataflowContext.of(graph));
		List<StreamSupplier<T>> suppliers = new ArrayList<>();

		for (StreamId streamId : inputStreamIds) {
			NodeUpload<T> nodeUpload = new NodeUpload<>(input.valueType(), streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = StreamSupplier.ofPromise(client.download(partition.getAddress(), streamId, input.valueType()));
			suppliers.add(supplier);
		}

		return StreamSupplier.concat(suppliers)
				.withEndOfStream(eos -> eos.whenException(e -> suppliers.forEach(supplier -> supplier.closeEx(e))));
	}
}
