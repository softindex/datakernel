/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.dataflow.graph;

import io.datakernel.async.process.Cancellable;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.collection.Try;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.dataflow.server.DataflowClient.Session;
import io.datakernel.dataflow.server.DataflowSerialization;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.*;
import static java.util.stream.Collectors.*;

/**
 * Represents a graph of partitions, nodes and streams in datagraph system.
 */
public class DataflowGraph {
	private final DataflowClient client;
	private final DataflowSerialization serialization;
	private final List<Partition> availablePartitions;
	private final Map<Node, Partition> nodePartitions = new LinkedHashMap<>();
	private final Map<StreamId, Node> streams = new LinkedHashMap<>();

	private final StructuredCodec<List<Node>> listNodeCodecs;

	public DataflowGraph(DataflowClient client, DataflowSerialization serialization, List<Partition> availablePartitions) {
		this.client = client;
		this.serialization = serialization;
		this.availablePartitions = availablePartitions;
		this.listNodeCodecs = indent(ofList(serialization.getNodeCodec()), "  ");
	}

	public List<Partition> getAvailablePartitions() {
		return availablePartitions;
	}

	public Partition getPartition(Node node) {
		return nodePartitions.get(node);
	}

	public Partition getPartition(StreamId streamId) {
		return getPartition(streams.get(streamId));
	}

	private Map<Partition, List<Node>> getNodesByPartition() {
		return nodePartitions.entrySet().stream()
				.collect(groupingBy(Map.Entry::getValue, mapping(Map.Entry::getKey, toList())));
	}

	private static class PartitionSession implements Cancellable {
		private final Partition partition;
		private final Session session;

		private PartitionSession(Partition partition, Session session) {
			this.partition = partition;
			this.session = session;
		}

		public Promise<Void> execute(List<Node> nodes) {
			return session.execute(nodes);
		}

		@Override
		public void close(@NotNull Throwable e) {
			session.close(e);
		}
	}

	/**
	 * Executes the defined operations on all partitions.
	 */
	public Promise<Void> execute() {
		Map<Partition, List<Node>> nodesByPartition = getNodesByPartition();

		return connect(nodesByPartition.keySet()).then(sessions ->
				Promises.all(sessions.stream().map(session -> {
					List<Node> nodes = nodesByPartition.get(session.partition);
					return session.execute(nodes);
				})).whenException($ -> sessions.forEach(Cancellable::close))
		);
	}

	private Promise<List<PartitionSession>> connect(Set<Partition> partitions) {
		return Promises.toList(partitions.stream()
				.map(partition -> client.connect(partition.getAddress()).map(session -> new PartitionSession(partition, session)).toTry()))
				.then(tries -> {
					List<PartitionSession> sessions = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());

					if (sessions.size() != partitions.size()) {
						sessions.forEach(Cancellable::close);
						return Promise.ofException(new Exception("Can't connect to all partitions"));
					}
					return Promise.of(sessions);
				});
	}

	public void addNode(Partition partition, Node node) {
		nodePartitions.put(node, partition);
		for (StreamId streamId : node.getOutputs()) {
			streams.put(streamId, node);
		}
	}

	public void addNodeStream(Node node, StreamId streamId) {
		streams.put(streamId, node);
	}

	public List<Partition> getPartitions(List<? extends StreamId> channels) {
		List<Partition> partitions = new ArrayList<>();
		for (StreamId streamId : channels) {
			Partition partition = getPartition(streamId);
			partitions.add(partition);
		}
		return partitions;
	}

	public DataflowSerialization getSerialization() {
		return serialization;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Map<Partition, List<Node>> map = getNodesByPartition();
		for (Partition partition : map.keySet()) {
			List<Node> nodes = map.get(partition);
			sb.append("--- ").append(partition).append("\n\n");
			sb.append(toJson(listNodeCodecs, nodes));
			sb.append("\n\n");
		}
		return sb.toString();
	}

}
