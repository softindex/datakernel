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

package io.datakernel.dataflow.server;

import io.datakernel.csp.binary.ByteBufsCodec;
import io.datakernel.csp.net.Messaging;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphCommandDownload;
import io.datakernel.dataflow.server.command.DatagraphCommandExecute;
import io.datakernel.dataflow.server.command.DatagraphResponse;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.net.AsyncTcpSocketNio;
import io.datakernel.promise.Promise;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import static io.datakernel.dataflow.server.Utils.nullTerminatedJson;

/**
 * Client for datagraph server.
 * Sends JSON commands for performing certain actions on server.
 */
public final class DataflowClient {

	private final DataflowSerialization serialization;
	private final ByteBufsCodec<DatagraphResponse, DatagraphCommand> codec;

	private final SocketSettings socketSettings = SocketSettings.createDefault();

	/**
	 * Constructs a datagraph client that runs in a given event loop and uses the specified DatagraphSerialization object for various serialization purposes.
	 *
	 * @param serialization DatagraphSerialization object used for serialization
	 */
	public DataflowClient(DataflowSerialization serialization) {
		this.serialization = serialization;
		this.codec = nullTerminatedJson(serialization.getResponseCodec(), serialization.getCommandCodec());
	}

	public <T> Promise<StreamSupplier<T>> download(InetSocketAddress address, StreamId streamId, Class<T> type) {
		return AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.then(socket -> {
					Messaging<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(socket, codec);
					DatagraphCommandDownload commandDownload = new DatagraphCommandDownload(streamId);

					return messaging.send(commandDownload)
							.map($ -> messaging.receiveBinaryStream()
									.transformWith(ChannelDeserializer.create(serialization.getBinarySerializer(type))
											.withExplicitEndOfStream())
									.withEndOfStream(eos -> eos
											.whenComplete(messaging::close))
							);
				});
	}

	public Promise<Void> execute(InetSocketAddress address, Collection<Node> nodes) {
		return AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.then(socket -> {
					Messaging<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(socket, codec);

					DatagraphCommandExecute commandExecute = new DatagraphCommandExecute(new ArrayList<>(nodes));
					return messaging.send(commandExecute)
							.then(messaging::sendEndOfStream);
				});
	}
}
