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

package io.datakernel.rpc.example;

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.net.ConnectSettings;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.client.sender.RequestSenderFactory;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcMessageSerializer;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory;
import io.datakernel.rpc.protocol.stream.RpcStreamProtocolSettings;
import io.datakernel.rpc.server.RequestHandlers;
import io.datakernel.rpc.server.RequestHandlers.RequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.rpc.client.sender.RequestSenderFactory.servers;

/**
 * This class contains static factory methods to setup RPC client and server.
 */
public final class CumulativeServiceHelper {
	public static final class ValueMessage extends RpcMessage.AbstractRpcMessage {
		@Serialize(order = 0)
		public int value;

		public ValueMessage(@Deserialize("value") int value) {
			this.value = value;
		}
	}

	public static final RpcMessageSerializer MESSAGE_SERIALIZER = messageSerializer();
	private static final RpcProtocolFactory PROTOCOL_FACTORY = new RpcStreamProtocolFactory(
			new RpcStreamProtocolSettings().packetSize(64 << 10, 64 << 10));

	private static RpcMessageSerializer messageSerializer() {
		return RpcMessageSerializer.builder().addExtraRpcMessageType(ValueMessage.class).build();
	}

	public static RequestHandlers cumulativeService() {
		return new RequestHandlers.Builder()
				.put(ValueMessage.class, new RequestHandler<ValueMessage>() {

					private final ValueMessage currentSum = new ValueMessage(0);

					@Override
					public void run(ValueMessage request, ResultCallback<RpcMessage.RpcMessageData> callback) {
						if (request.value != 0) {
							currentSum.value += request.value;
						} else {
							currentSum.value = 0;
						}
						callback.onResult(currentSum);
					}
				}).build();
	}

	public static RpcServer createServer(NioEventloop eventloop, int listenPort) {
		RpcServer server = new RpcServer.Builder(eventloop)
				.serializer(CumulativeServiceHelper.MESSAGE_SERIALIZER)
				.requestHandlers(cumulativeService())
				.protocolFactory(PROTOCOL_FACTORY)
				.build();
		return server.setListenPort(listenPort);
	}

	public static RpcClient createClient(NioEventloop eventloop, List<InetSocketAddress> addresses, ConnectSettings connectSettings) {
		return new RpcClient.Builder(eventloop)
				.addresses(addresses)
				.connectSettings(connectSettings)
				.waitForAllConnected()
				.serializer(CumulativeServiceHelper.MESSAGE_SERIALIZER)
				.requestSenderFactory(RequestSenderFactory.firstAvailable(servers(addresses)))
				.protocolFactory(PROTOCOL_FACTORY)
				.build();
	}

	public static int calculateRpcMessageSize(ValueMessage message) {
		BufferSerializer<RpcMessage> serializer = MESSAGE_SERIALIZER.getSerializer();
		int defaultBufferSize = 100;
		SerializationOutputBuffer output;
		while (true) {
			try {
				byte[] array = new byte[defaultBufferSize];
				output = new SerializationOutputBuffer(array);
				serializer.serialize(output, new RpcMessage(12345, message));
				break;
			} catch (ArrayIndexOutOfBoundsException e) {
				defaultBufferSize = defaultBufferSize * 2;
			}
		}
		return output.position();
	}
}
