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

package io.datakernel.rpc.protocol.stream;

import io.datakernel.rpc.protocol.RpcConnection;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocol;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.processor.StreamBinarySerializer;

import java.nio.channels.SocketChannel;

import static io.datakernel.util.Preconditions.checkArgument;

public final class RpcStreamProtocolFactory implements RpcProtocolFactory {
	public static final int DEFAULT_PACKET_SIZE = 16;
	public static final int MAX_PACKET_SIZE = StreamBinarySerializer.MAX_SIZE;

	private int defaultPacketSize = DEFAULT_PACKET_SIZE;
	private int maxPacketSize = MAX_PACKET_SIZE;
	private boolean compression = true;

	private RpcStreamProtocolFactory() {
	}

	public RpcStreamProtocolFactory(int defaultPacketSize, int maxPacketSize, boolean compression) {
		checkArgument(defaultPacketSize > 0);
		checkArgument(maxPacketSize >= defaultPacketSize);
		this.defaultPacketSize = defaultPacketSize;
		this.maxPacketSize = maxPacketSize;
		this.compression = compression;
	}

	public static RpcStreamProtocolFactory streamProtocol() {
		return new RpcStreamProtocolFactory();
	}

	public static RpcStreamProtocolFactory streamProtocol(int defaultPacketSize, int maxPacketSize, boolean compression) {
		return new RpcStreamProtocolFactory(defaultPacketSize, maxPacketSize, compression);
	}

	public RpcProtocol create(final RpcConnection connection, SocketChannel socketChannel,
	                          BufferSerializer<RpcMessage> messageSerializer,
	                          boolean isServer) {
		return new RpcStreamProtocol(connection.getEventloop(), socketChannel,
				messageSerializer,
				defaultPacketSize, maxPacketSize, compression) {
			@Override
			protected void onReceiveMessage(RpcMessage message) {
				connection.onReceiveMessage(message);
			}

			@Override
			protected void onWired() {
				connection.ready();
			}

			@Override
			protected void onClosed() {
				connection.onClosed();
			}
		};
	}
}
