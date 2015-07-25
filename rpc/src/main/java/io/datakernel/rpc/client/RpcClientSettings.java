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

package io.datakernel.rpc.client;

import com.google.common.collect.ImmutableList;
import io.datakernel.eventloop.SocketReconnector;
import io.datakernel.net.ConnectSettings;
import io.datakernel.net.SocketSettings;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public final class RpcClientSettings {
	private static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
	private static final int DEFAULT_RECONNECT_INTERVAL = 30 * 1000;
	public static final ConnectSettings DEFAULT_CONNECT_SETTINGS = new ConnectSettings(DEFAULT_CONNECT_TIMEOUT, SocketReconnector.RECONNECT_ALWAYS, DEFAULT_RECONNECT_INTERVAL);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);
	public static final long DEFAULT_PING_INTERVAL_MILLIS = 30_000L;
	public static final int DEFAULT_PING_AMOUNT_FAILED = 3;
	public static final int DEFAULT_ALL_CONNECTIONS = Integer.MAX_VALUE;
	public static final int DEFAULT_TIMEOUT_PRECISION = 10; //ms

	private List<InetSocketAddress> addresses;
	private List<InetSocketAddress> standbyAddresses;
	private List<List<InetSocketAddress>> partitionsAddresses;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private ConnectSettings connectSettings = DEFAULT_CONNECT_SETTINGS;
	private long pingIntervalMillis = DEFAULT_PING_INTERVAL_MILLIS;
	private int pingAmountFailed = DEFAULT_PING_AMOUNT_FAILED;
	private int minAliveConnections = DEFAULT_ALL_CONNECTIONS;
	private int timeoutPrecision = DEFAULT_TIMEOUT_PRECISION;

	public RpcClientSettings addresses(List<InetSocketAddress> addresses) {
		checkState(partitionsAddresses == null, "partitionsAddresses is set");
		checkNotNull(addresses);
		checkArgument(!addresses.isEmpty());
		this.addresses = addresses;
		return this;
	}

	public RpcClientSettings standbyAddresses(List<InetSocketAddress> standbyAddresses) {
		checkState(partitionsAddresses == null, "partitionsAddresses is set");
		checkNotNull(standbyAddresses);
		checkArgument(!standbyAddresses.isEmpty());
		this.standbyAddresses = standbyAddresses;
		return this;
	}

	public RpcClientSettings addPartition(List<InetSocketAddress> partitionAddresses) {
		checkState(addresses == null, "addresses is set");
		checkState(standbyAddresses == null, "standby addresses is set");
		if (partitionsAddresses == null) {
			partitionsAddresses = new ArrayList<>();
		}
		partitionsAddresses.add(partitionAddresses);
		return this;
	}

	public RpcClientSettings socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return this;
	}

	public RpcClientSettings connectSettings(ConnectSettings connectSettings) {
		this.connectSettings = checkNotNull(connectSettings);
		return this;
	}

	public RpcClientSettings pingIntervalMillis(long pingInterval) {
		this.pingIntervalMillis = checkNotNull(pingInterval);
		return this;
	}

	public RpcClientSettings pingAmountFailed(int pingAmountFailed) {
		checkArgument(pingAmountFailed > 0);
		this.pingAmountFailed = pingAmountFailed;
		return this;
	}

	public RpcClientSettings minAliveConnections(int minAliveConnections) {
		checkArgument(minAliveConnections >= 0);
		this.minAliveConnections = minAliveConnections;
		return this;
	}

	public RpcClientSettings timeoutPrecision(int timeoutPrecision) {
		checkArgument(timeoutPrecision >= 0);
		this.timeoutPrecision = timeoutPrecision;
		return this;
	}

	public List<InetSocketAddress> getAddresses() {
		checkState(partitionsAddresses == null, "partitionsAddresses is set");
		return addresses;
	}

	public List<InetSocketAddress> getStandbyAddresses() {
		checkState(partitionsAddresses == null, "partitionsAddresses is set");
		return standbyAddresses;
	}

	public List<List<InetSocketAddress>> getPartitionsAddresses() {
		checkState(addresses == null, "addresses is set");
		checkState(standbyAddresses == null, "standby addresses is set");
		return partitionsAddresses;
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	public ConnectSettings getConnectSettings() {
		return connectSettings;
	}

	public long getPingIntervalMillis() {
		return pingIntervalMillis;
	}

	public int getPingAmountFailed() {
		return pingAmountFailed;
	}

	public int getMinAliveConnections() {
		return minAliveConnections;
	}

	public int getTimeoutPrecision() {
		return timeoutPrecision;
	}

	private RpcClientSettings getCopy() {
		RpcClientSettings copy = new RpcClientSettings();
		copy.socketSettings = this.socketSettings;
		copy.connectSettings = this.connectSettings;
		copy.pingIntervalMillis = this.pingIntervalMillis;
		copy.pingAmountFailed = this.pingAmountFailed;
		copy.minAliveConnections = this.minAliveConnections;
		copy.timeoutPrecision = this.timeoutPrecision;
		return copy;
	}

	public List<RpcClientSettings> getPartitionSettings() {
		ImmutableList.Builder<RpcClientSettings> result = new ImmutableList.Builder<>();
		for (List<InetSocketAddress> partitionAddresses : getPartitionsAddresses()) {
			RpcClientSettings partitionSettings = getCopy()
					.addresses(partitionAddresses)
					.minAliveConnections(Math.min(this.getMinAliveConnections(), partitionAddresses.size()));
			result.add(partitionSettings);
		}
		return result.build();
	}

	public RpcClientSettings getStandbySettings() {
		if (getStandbyAddresses() == null)
			return null;
		return getCopy().addresses(getStandbyAddresses());
	}
}
