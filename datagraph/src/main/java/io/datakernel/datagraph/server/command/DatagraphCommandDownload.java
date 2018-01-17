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

package io.datakernel.datagraph.server.command;

import io.datakernel.datagraph.graph.StreamId;

public final class DatagraphCommandDownload extends DatagraphCommand {
	private StreamId streamId;

	public DatagraphCommandDownload() {
	}

	public DatagraphCommandDownload(StreamId streamId) {
		this.setStreamId(streamId);
	}

	public StreamId getStreamId() {
		return streamId;
	}

	public void setStreamId(StreamId streamId) {
		this.streamId = streamId;
	}

	@Override
	public String toString() {
		return "DatagraphCommandDownload{streamId=" + streamId + "} ";
	}
}
