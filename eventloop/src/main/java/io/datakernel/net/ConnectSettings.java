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

package io.datakernel.net;

import static com.google.common.base.Preconditions.checkArgument;

public final class ConnectSettings {
	public static final int DEFAULT_RECONNECT_MILLIS = 30_000;

	private final int connectTimeoutMillis;
	private final int attemptsReconnection;
	private final int reconnectIntervalMillis;

	public ConnectSettings() {
		this(0); // infinite timeout
	}

	public ConnectSettings(int connectTimeoutMillis) {
		this(connectTimeoutMillis, 0, DEFAULT_RECONNECT_MILLIS);
	}

	public ConnectSettings(int connectTimeoutMillis, int attemptsReconnection, int reconnectIntervalMillis) {
		checkArgument(connectTimeoutMillis >= 0, "connectTimeoutMillis must be positive value, got %s", connectTimeoutMillis);
		checkArgument(attemptsReconnection >= 0, "attemptsReconnection must be positive value, got %s", attemptsReconnection);
		checkArgument(reconnectIntervalMillis >= 0, "reconnectIntervalMillis must be positive value, got %s", reconnectIntervalMillis);
		this.connectTimeoutMillis = connectTimeoutMillis;
		this.attemptsReconnection = attemptsReconnection;
		this.reconnectIntervalMillis = reconnectIntervalMillis;
	}

	public ConnectSettings connectTimeoutMillis(int connectTimeoutMillis) {
		return new ConnectSettings(connectTimeoutMillis, attemptsReconnection, reconnectIntervalMillis);
	}

	public ConnectSettings attemptsReconnection(int attemptsReconnection) {
		return new ConnectSettings(connectTimeoutMillis, attemptsReconnection, reconnectIntervalMillis);
	}

	public ConnectSettings reconnectIntervalMillis(int reconnectIntervalMillis) {
		return new ConnectSettings(connectTimeoutMillis, attemptsReconnection, reconnectIntervalMillis);
	}

	public int connectTimeoutMillis() {
		return connectTimeoutMillis;
	}

	public int attemptsReconnection() {
		return attemptsReconnection;
	}

	public int reconnectIntervalMillis() {
		return reconnectIntervalMillis;
	}

}
