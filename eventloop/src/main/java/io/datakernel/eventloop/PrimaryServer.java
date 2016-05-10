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

import java.nio.channels.SocketChannel;
import java.util.Collection;

/**
 * It is the {@link AbstractServer} which only handles accepting to it. It contains collection of
 * other {@link EventloopServer}, and when takes place new accept to it, it forwards request to other server
 * from collection with round-robin algorithm.
 */
public final class PrimaryServer extends AbstractServer<PrimaryServer> {
	private EventloopServer[] workerServers;

	private int currentAcceptor = 0;

	private PrimaryServer(Eventloop primaryEventloop) {
		super(primaryEventloop);
	}

	/**
	 * Creates a new PrimaryNioServer with specify Eventloop
	 *
	 * @param primaryEventloop the Eventloop which will execute IO tasks of this server
	 * @return new PrimaryNioServer
	 */
	public static PrimaryServer create(Eventloop primaryEventloop) {
		return new PrimaryServer(primaryEventloop);
	}

	/**
	 * Adds the list of NioServers which will handle accepting to this server
	 *
	 * @param workerServers list of workers NioServers
	 * @return this PrimaryNioServer
	 */
	@SuppressWarnings("unchecked")
	public PrimaryServer workerServers(Collection<? extends EventloopServer> workerServers) {
		this.workerServers = workerServers.toArray(new EventloopServer[workerServers.size()]);
		return this;
	}

	/**
	 * Adds the NioServers from argument which will handle accepting to this server
	 *
	 * @param workerServers list of workers NioServers
	 * @return this PrimaryNioServer
	 */
	@SuppressWarnings("unchecked")
	public PrimaryServer workerServers(EventloopServer... workerServers) {
		this.workerServers = workerServers;
		return this;
	}

	/**
	 * Instance of this class can not create a connection, it only forwards it. That is why this method
	 * throws Exception
	 *
	 * @param socketChannel the socketChannel for creating connection.
	 */
	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		throw new UnsupportedOperationException();
	}

	/**
	 * On accepting this server forwards a received socketChannel, and calls onAccept() of the specify worker server.
	 * Method uses round-robin algorithm to select to which server task will be forwarded
	 *
	 * @param socketChannel the incoming socketChannel.
	 */
	@Override
	protected void doAccept(final SocketChannel socketChannel) {
		// jmx
		getTotalAccepts().recordEvent();

		final EventloopServer server = workerServers[currentAcceptor];
		currentAcceptor = (currentAcceptor + 1) % workerServers.length;
		Eventloop eventloop = server.getEventloop();
		if (eventloop == this.eventloop) {
			server.onAccept(socketChannel);
		} else {
			eventloop.execute(new Runnable() {
				@Override
				public void run() {
					server.onAccept(socketChannel);
				}
			});
		}

		if (acceptOnce) {
			close();
		}
	}
}
