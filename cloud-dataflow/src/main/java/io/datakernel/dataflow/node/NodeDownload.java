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
import io.datakernel.dataflow.server.DatagraphClient;
import io.datakernel.datastream.StreamSupplier;

import java.net.InetSocketAddress;
import java.util.Collection;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which downloads data from a given address and stream.
 *
 * @param <T> data items type
 */
public final class NodeDownload<T> implements Node {
	private final Class<T> type;
	private final InetSocketAddress address;
	private final StreamId streamId;
	private final StreamId output;

	public NodeDownload(Class<T> type, InetSocketAddress address, StreamId streamId, StreamId output) {
		this.type = type;
		this.address = address;
		this.streamId = streamId;
		this.output = output;
	}

	public NodeDownload(Class<T> type, InetSocketAddress address, StreamId streamId) {
		this.type = type;
		this.address = address;
		this.streamId = streamId;
		this.output = new StreamId();
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(streamId);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		DatagraphClient client = taskContext.environment().getInstance(DatagraphClient.class);
		StreamSupplier<T> stream = StreamSupplier.ofPromise(client.download(address, streamId, type));
		taskContext.export(output, stream);
	}

	public Class<T> getType() {
		return type;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public StreamId getStreamId() {
		return streamId;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeDownload{type=" + type + ", address=" + address + ", streamId=" + streamId + ", output=" + output + '}';
	}
}
