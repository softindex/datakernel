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

package io.datakernel.datagraph.graph;

import com.google.common.reflect.TypeToken;
import io.datakernel.datagraph.node.Node;
import io.datakernel.datagraph.server.DatagraphSerialization;

import java.util.*;

/**
 * Represents a graph of partitions, nodes and streams in datagraph system.
 */
@SuppressWarnings("unchecked")
public class DataGraph {
	private final DatagraphSerialization serialization;
	private final List<Partition> availablePartitions;
	private final Map<Node, Partition> nodePartitions = new LinkedHashMap<>();
	private final Map<StreamId, Node> streams = new LinkedHashMap<>();

	public DataGraph(DatagraphSerialization serialization, List<Partition> availablePartitions) {
		this.serialization = serialization;
		this.availablePartitions = availablePartitions;
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
		Map<Partition, List<Node>> listMultimap = new HashMap<>();
		for (Map.Entry<Node, Partition> entry : nodePartitions.entrySet()) {
			listMultimap.computeIfAbsent(entry.getValue(), partition -> new ArrayList<>()).add(entry.getKey());
		}
		return listMultimap;
	}

	/**
	 * Executes the defined operations on all partitions.
	 */
	public void execute() {
		Map<Partition, List<Node>> map = getNodesByPartition();
		for (Partition partition : map.keySet()) {
			List<Node> nodes = map.get(partition);
			partition.execute(nodes);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Map<Partition, List<Node>> map = getNodesByPartition();
		for (Partition partition : map.keySet()) {
			sb.append("--- ").append(partition).append("\n\n");
			List<Node> nodes = map.get(partition);
			String str = serialization.gson.toJson(nodes, new TypeToken<List<Node>>() {
			}.getType());
			sb.append(str).append("\n\n");
		}
		return sb.toString();
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

	public DatagraphSerialization getSerialization() {
		return serialization;
	}
}
