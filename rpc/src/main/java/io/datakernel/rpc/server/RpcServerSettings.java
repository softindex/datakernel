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

package io.datakernel.rpc.server;

import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcServerSettings {
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = new ServerSocketSettings(16384);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);

	private List<InetSocketAddress> listenAddresses;
	private ServerSocketSettings serverSocketSettings = DEFAULT_SERVER_SOCKET_SETTINGS;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	public RpcServerSettings listenPort(int port) {
		return listenAddresses(Collections.singletonList(new InetSocketAddress(port)));
	}

	public RpcServerSettings listenAddresses(List<InetSocketAddress> listenAddresses) {
		checkNotNull(listenAddresses);
		checkArgument(!listenAddresses.isEmpty());
		this.listenAddresses = listenAddresses;
		return this;
	}

	public RpcServerSettings serverSocketSettings(ServerSocketSettings serverSocketSettings) {
		this.serverSocketSettings = checkNotNull(serverSocketSettings);
		return this;
	}

	public RpcServerSettings socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return this;
	}

	public List<InetSocketAddress> getListenAddresses() {
		return listenAddresses;
	}

	public ServerSocketSettings getServerSocketSettings() {
		return serverSocketSettings;
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}
}
