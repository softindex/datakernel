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

package io.datakernel.hashfs2;

import java.net.InetSocketAddress;

import static io.datakernel.hashfs2.ServerStatus.RUNNING;

public final class ServerInfo {
	private final InetSocketAddress address;
	private final int serverId;
	private final double weight;
	private final State state;

	public ServerInfo(int serverId, InetSocketAddress address, double weight) {
		this.address = address;
		this.serverId = serverId;
		this.weight = weight;
		state = new State();
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public int getServerId() {
		return serverId;
	}

	public double getWeight() {
		return weight;
	}

	public void updateState(ServerStatus status, long heartBeat) {
		if (status != null && heartBeat > state.lastHeartBeatReceived)
			state.update(status, heartBeat);
	}

	// TODO (arashev) better name for parameter - represents current time + timeout after which we consider server to be shut down
	public boolean isAlive(long maximumDieTime) {
		return state.isAlive(maximumDieTime);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ServerInfo that = (ServerInfo) o;
		return serverId == that.serverId;
	}

	@Override
	public int hashCode() {
		return serverId;
	}

	@Override
	public String toString() {
		return "FileServer id: " + serverId;
	}

	private class State {
		private ServerStatus serverStatus;
		private long lastHeartBeatReceived;

		void update(ServerStatus serverStatus, long lastHeartBeat) {
			this.serverStatus = serverStatus;
			this.lastHeartBeatReceived = lastHeartBeat;
		}

		boolean isAlive(long maximumDieTime) {
			return serverStatus == RUNNING
					&& lastHeartBeatReceived < maximumDieTime;
		}
	}
}
