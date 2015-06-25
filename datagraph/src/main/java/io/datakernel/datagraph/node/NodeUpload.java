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

import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.graph.TaskContext;
import io.datakernel.datagraph.server.DatagraphServer;

import java.util.Collection;
import java.util.Collections;

/**
 * Represents a node, which uploads data to a stream.
 *
 * @param <T> data items type
 */
public final class NodeUpload<T> implements Node {
	private final Class type;
	private final StreamId streamId;

	public NodeUpload(Class<T> type, StreamId streamId) {
		this.type = type;
		this.streamId = streamId;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return Collections.emptyList();
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		DatagraphServer server = taskContext.environment().getInstance(DatagraphServer.class);
		taskContext.bindChannel(streamId, server.upload(streamId, type));
	}

	public StreamId getStreamId() {
		return streamId;
	}

	@Override
	public String toString() {
		return "NodeUpload{type=" + type + ", streamId=" + streamId + '}';
	}
}
