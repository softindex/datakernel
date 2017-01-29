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

import java.util.List;

/**
 * It is the {@link AbstractServer} which only handles accepting to it. It contains collection of
 * other {@link EventloopServer}, and when takes place new accept to it, it forwards request to other server
 * from collection with round-robin algorithm.
 */
public final class PrimaryServer extends AbstractServer<PrimaryServer> {

	private final WorkerServer[] workerServers;

	private int currentAcceptor = -1; // first server index is currentAcceptor + 1

	// region builders
	private PrimaryServer(Eventloop primaryEventloop, WorkerServer[] workerServers) {
		super(primaryEventloop);
		this.workerServers = workerServers;
		for (WorkerServer workerServer : workerServers) {
			if (workerServer instanceof AbstractServer) {
				((AbstractServer) workerServer).acceptServer = this;
			}
		}
	}

	public static PrimaryServer create(Eventloop primaryEventloop, List<? extends WorkerServer> workerServers) {
		return create(primaryEventloop, workerServers.toArray(new WorkerServer[workerServers.size()]));
	}

	public static PrimaryServer create(Eventloop primaryEventloop, WorkerServer... workerServer) {
		return new PrimaryServer(primaryEventloop, workerServer);
	}
	// endregion

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected WorkerServer getWorkerServer() {
		currentAcceptor = (currentAcceptor + 1) % workerServers.length;
		return workerServers[currentAcceptor];
	}
}
