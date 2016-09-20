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
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * It is the {@link AbstractServer} which only handles accepting to it. It contains collection of
 * other {@link EventloopServer}, and when takes place new accept to it, it forwards request to other server
 * from collection with round-robin algorithm.
 */
public final class PrimaryServer extends AbstractServer<PrimaryServer> {
	private final EventloopServer[] workerServers;

	private int currentAcceptor = 0;

	// region builders
	private PrimaryServer(Eventloop primaryEventloop, EventloopServer[] workerServers) {
		super(primaryEventloop);
		this.workerServers = workerServers;
	}

	private PrimaryServer(Eventloop eventloop, ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                      boolean acceptOnce,
	                      Collection<InetSocketAddress> listenAddresses,
	                      InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                      SSLContext sslContext, ExecutorService sslExecutor,
	                      Collection<InetSocketAddress> sslListenAddresses,
	                      PrimaryServer previousInstance) {
		super(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses);
		this.workerServers = previousInstance.workerServers;
	}

	public static PrimaryServer create(Eventloop primaryEventloop, List<? extends EventloopServer> workerServers) {
		EventloopServer[] workerServersArr = workerServers.toArray(new EventloopServer[workerServers.size()]);
		return new PrimaryServer(primaryEventloop, workerServersArr);
	}

	@Override
	protected PrimaryServer recreate(Eventloop eventloop, ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                                 boolean acceptOnce,
	                                 Collection<InetSocketAddress> listenAddresses,
	                                 InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                                 SSLContext sslContext, ExecutorService sslExecutor,
	                                 Collection<InetSocketAddress> sslListenAddresses) {
		return new PrimaryServer(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses, this);
	}
	// endregion

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected EventloopServer getWorkerServer() {
		return workerServers[(currentAcceptor++) % workerServers.length];
	}

}
