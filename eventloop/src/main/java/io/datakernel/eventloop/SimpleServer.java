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

package io.datakernel.eventloop;

import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public final class SimpleServer extends AbstractServer<SimpleServer> {
	private final SocketHandlerProvider socketHandlerProvider;

	private SimpleServer(Eventloop eventloop, ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                     boolean acceptOnce,
	                     Collection<InetSocketAddress> listenAddresses,
	                     InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                     SSLContext sslContext, ExecutorService sslExecutor,
	                     Collection<InetSocketAddress> sslListenAddresses,
	                     SimpleServer previousInstance) {
		super(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses);
		this.socketHandlerProvider = previousInstance.socketHandlerProvider;
	}

	private SimpleServer(Eventloop eventloop, SocketHandlerProvider socketHandlerProvider) {
		super(eventloop);
		this.socketHandlerProvider = socketHandlerProvider;
	}

	public static SimpleServer create(Eventloop eventloop, SocketHandlerProvider socketHandlerProvider) {
		return new SimpleServer(eventloop, socketHandlerProvider);
	}

	@Override
	protected SimpleServer recreate(Eventloop eventloop, ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                                boolean acceptOnce,
	                                Collection<InetSocketAddress> listenAddresses,
	                                InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                                SSLContext sslContext, ExecutorService sslExecutor,
	                                Collection<InetSocketAddress> sslListenAddresses) {
		return new SimpleServer(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses, this);
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		return socketHandlerProvider.createSocketHandler(asyncTcpSocket);
	}

	public interface SocketHandlerProvider {
		AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket socket);
	}
}
