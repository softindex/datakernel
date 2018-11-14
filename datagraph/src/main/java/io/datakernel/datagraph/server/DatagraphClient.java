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

package io.datakernel.datagraph.server;

import io.datakernel.async.Promise;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.Node;
import io.datakernel.datagraph.server.command.DatagraphCommand;
import io.datakernel.datagraph.server.command.DatagraphCommandDownload;
import io.datakernel.datagraph.server.command.DatagraphCommandExecute;
import io.datakernel.datagraph.server.command.DatagraphResponse;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.net.SocketSettings;
import io.datakernel.serial.net.ByteBufSerializer;
import io.datakernel.serial.net.MessagingWithBinaryStreaming;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.stream.StreamSupplier;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import static io.datakernel.serial.net.ByteBufSerializers.ofJson;

/**
 * Client for datagraph server.
 * Sends JSON commands for performing certain actions on server.
 */
public final class DatagraphClient {

	private final DatagraphSerialization serialization;
	private final ByteBufSerializer<DatagraphResponse, DatagraphCommand> serializer;

	private final SocketSettings socketSettings = SocketSettings.create();

	/**
	 * Constructs a datagraph client that runs in a given event loop and uses the specified DatagraphSerialization object for various serialization purposes.
	 *
	 * @param serialization DatagraphSerialization object used for serialization
	 */
	public DatagraphClient(DatagraphSerialization serialization) {
		this.serialization = serialization;
		this.serializer = ofJson(serialization.responseAdapter, serialization.commandAdapter);
	}

	public <T> Promise<StreamSupplier<T>> download(InetSocketAddress address, StreamId streamId, Class<T> type) {
		return AsyncTcpSocketImpl.connect(address, 0, socketSettings)
				.thenCompose(socket -> {
					MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(socket, serializer);
					DatagraphCommandDownload commandDownload = new DatagraphCommandDownload(streamId);

					return messaging.send(commandDownload)
							.thenApply($ -> messaging.receiveBinaryStream()
									.apply(SerialBinaryDeserializer.create(serialization.getSerializer(type)))
									.withEndOfStream(eos -> eos
											.whenComplete(($1, e1) -> messaging.close()))
									.withLateBinding());
				});
	}

	public Promise<Void> execute(InetSocketAddress address, Collection<Node> nodes) {
		return AsyncTcpSocketImpl.connect(address, 0, socketSettings)
				.thenCompose(socket -> {
					MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(socket, serializer);

					DatagraphCommandExecute commandExecute = new DatagraphCommandExecute(new ArrayList<>(nodes));
					return messaging.send(commandExecute)
							.thenCompose($ -> messaging.sendEndOfStream());
				});
	}
}
