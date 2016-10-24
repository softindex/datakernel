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

import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.protocol.RpcConnection;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocol;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.util.MemSize;

import static io.datakernel.util.Preconditions.checkArgument;

public final class RpcStreamProtocolFactory implements RpcProtocolFactory {
	public static final int DEFAULT_PACKET_SIZE = 16;
	public static final int MAX_PACKET_SIZE = StreamBinarySerializer.MAX_SIZE;

	private int defaultPacketSize = DEFAULT_PACKET_SIZE;
	private int maxPacketSize = MAX_PACKET_SIZE;
	private boolean compression = false;

	private RpcStreamProtocolFactory() {
	}

	private RpcStreamProtocolFactory(int defaultPacketSize, int maxPacketSize, boolean compression) {
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

	public static RpcStreamProtocolFactory streamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		return new RpcStreamProtocolFactory((int) defaultPacketSize.get(), (int) maxPacketSize.get(), compression);
	}

	@Override
	public RpcProtocol createServerProtocol(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                          RpcConnection connection, BufferSerializer<RpcMessage> messageSerializer) {
		return new RpcStreamProtocol(eventloop, asyncTcpSocket,
				connection, messageSerializer,
				defaultPacketSize, maxPacketSize, compression, true);
	}

	@Override
	public RpcProtocol createClientProtocol(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                                        RpcConnection connection, BufferSerializer<RpcMessage> messageSerializer) {
		return new RpcStreamProtocol(eventloop, asyncTcpSocket,
				connection, messageSerializer,
				defaultPacketSize, maxPacketSize, compression, false);
	}
}
