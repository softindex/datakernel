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

import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.collection.Try;
import io.datakernel.common.ref.RefInt;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.node.NodeDownload;
import io.datakernel.dataflow.node.NodeShard;
import io.datakernel.dataflow.node.NodeUpload;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.dataflow.server.DataflowClient.Session;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static java.util.stream.Collectors.*;

/**
 * Represents a graph of partitions, nodes and streams in datagraph system.
 */
public final class DataflowGraph {
	private final Map<Node, Partition> nodePartitions = new LinkedHashMap<>();
	private final Map<StreamId, Node> streams = new LinkedHashMap<>();

	private final DataflowClient client;
	private final List<Partition> availablePartitions;
	private final StructuredCodec<List<Node>> listNodeCodec;

	public DataflowGraph(DataflowClient client, List<Partition> availablePartitions, StructuredCodec<Node> nodeCodec) {
		this.client = client;
		this.availablePartitions = availablePartitions;
		this.listNodeCodec = ofList(nodeCodec);
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

	private static class PartitionSession implements AsyncCloseable {
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
		public void closeEx(@NotNull Throwable e) {
			session.closeEx(e);
		}
	}

	/**
	 * Executes the defined operations on all partitions.
	 */
	public Promise<Void> execute() {
		Map<Partition, List<Node>> nodesByPartition = getNodesByPartition();
		return connect(nodesByPartition.keySet())
				.then(sessions ->
						Promises.all(
								sessions.stream()
										.map(session -> session.execute(nodesByPartition.get(session.partition))))
								.whenException(() -> sessions.forEach(PartitionSession::close))
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
						sessions.forEach(PartitionSession::close);
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

	public String toGraphViz() {
		return toGraphViz(false, 2);
	}

	public String toGraphViz(boolean streamLabels) {
		return toGraphViz(streamLabels, 2);
	}

	public String toGraphViz(int maxPartitions) {
		return toGraphViz(false, maxPartitions);
	}

	public String toGraphViz(boolean streamLabels, int maxPartitions) {
		StringBuilder sb = new StringBuilder("digraph {\n\n");

		RefInt nodeCounter = new RefInt(0);
		RefInt clusterCounter = new RefInt(0);

		Map<StreamId, Node> nodesByInput = new HashMap<>();
		Map<StreamId, StreamId> network = new HashMap<>();

		List<NodeUpload<?>> uploads = new ArrayList<>();

		// collect network streams and populate the nodesByInput lookup map
		for (Node node : nodePartitions.keySet()) {
			if (node instanceof NodeDownload) {
				NodeDownload<?> download = (NodeDownload<?>) node;
				network.put(download.getStreamId(), download.getOutput());
			} else if (node instanceof NodeUpload) {
				uploads.add((NodeUpload<?>) node);
			} else {
				node.getInputs().forEach(input -> nodesByInput.put(input, node));
			}
		}
		// check for upload nodes not connected to download ones, add them
		for (NodeUpload<?> upload : uploads) {
			StreamId streamId = upload.getStreamId();
			if (!network.containsKey(streamId)) {
				nodesByInput.put(streamId, upload);
			}
		}

		Map<Node, String> nodeIds = new HashMap<>();

		// define nodes and group them by partitions using graphviz clusters
		getNodesByPartition()
				.entrySet()
				.stream()
				.limit(maxPartitions)
				.forEach(e -> {
					sb.append("  subgraph cluster_")
							.append(++clusterCounter.value)
							.append(" {\n")
							.append("    label=\"")
							.append(e.getKey().getAddress())
							.append("\";\n    style=rounded;\n\n");
					for (Node node : e.getValue()) {
						// upload and download nodes have no common connections
						// download nodes are never drawn, and upload only has an input
						if ((node instanceof NodeDownload || (node instanceof NodeUpload && network.containsKey(((NodeUpload<?>) node).getStreamId())))) {
							continue;
						}
						String nodeId = "n" + ++nodeCounter.value;
						sb.append("    ")
								.append(nodeId)
								.append(" [label=\"")
								.append(node.getClass().getSimpleName())
								.append("\"];\n");
						nodeIds.put(node, nodeId);
					}
					sb.append("  }\n\n");
				});

		Set<String> notFound = new HashSet<>();

		// walk over each node outputs and build the connections
		nodeIds.forEach((node, id) -> {
			for (StreamId output : node.getOutputs()) {
				Node outputNode = nodesByInput.get(output);
				boolean net = false;
				boolean forceLabel = false;
				// check if unbound stream is a network one
				StreamId prev = null;
				if (outputNode == null) {
					StreamId through = network.get(output);
					if (through != null) {
						prev = output;
						output = through;
						// connect to network stream output and
						// set the 'net' flag to true for dashed arrow
						outputNode = nodesByInput.get(output);
						net = outputNode != null;
					}
				}
				String nodeId = nodeIds.get(outputNode);
				// if still unbound and not net (because of partition limit),
				// draw as point and force the stream label
				if (nodeId == null && !net) {
					nodeId = "s" + output.getId();
					notFound.add(nodeId);
					forceLabel = true;
				}
				if (nodeId != null) { // nodeId might be null only for net nodes here, see previous 'if'
					sb.append("  ")
							.append(id)
							.append(" -> ")
							.append(nodeId)
							.append(" [");
					if (streamLabels || forceLabel) {
						if (prev != null) {
							sb.append("taillabel=\"")
									.append(prev)
									.append("\", headlabel=\"")
									.append(output)
									.append("\"");
						} else {
							sb.append("xlabel=\"")
									.append(output)
									.append("\"");
						}
						if (net) {
							sb.append(", ");
						}
					}
					if (net) {
						sb.append("style=dashed");
					}
					sb.append("];\n");
				}
			}
		});

		// draw the nodes that were never defined as points that still have connections
		if (!notFound.isEmpty()) {
			sb.append('\n');
			notFound.forEach(id -> sb.append("  ").append(id).append(" [shape=point];\n"));
		}

		sb.append("}");

		return sb.toString();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Map<Partition, List<Node>> map = getNodesByPartition();
		for (Partition partition : map.keySet()) {
			List<Node> nodes = map.get(partition);
			sb.append("--- ").append(partition).append("\n\n");
			sb.append(toJson(listNodeCodec, nodes));
			sb.append("\n\n");
		}
		return sb.toString();
	}
}
