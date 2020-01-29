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
import io.datakernel.dataflow.server.DataflowServer;

import java.util.Collection;
import java.util.Collections;

/**
 * Represents a node, which uploads data to a stream.
 *
 * @param <T> data items type
 */
public final class NodeUpload<T> implements Node {
	private Class<T> type;
	private StreamId streamId;

	public NodeUpload() {
	}

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
		DataflowServer server = taskContext.environment().getInstance(DataflowServer.class);
		taskContext.bindChannel(streamId, server.upload(streamId, type));
	}

	public Class<T> getType() {
		return type;
	}

	public void setType(Class<T> type) {
		this.type = type;
	}

	public StreamId getStreamId() {
		return streamId;
	}

	public void setStreamId(StreamId streamId) {
		this.streamId = streamId;
	}

	@Override
	public String toString() {
		return "NodeUpload{type=" + type + ", streamId=" + streamId + '}';
	}
}
