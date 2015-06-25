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

package io.datakernel.hashfs;

public class Configuration {

	private int replicas = 3;
	private long tickPeriod = 50L;
	private long serverDieTime = 150L;
	private long serverDiscoveryTime = 2000L;
	private int maxRetryAttempts = 3;

	public int getReplicas() {
		return replicas;
	}

	public Configuration setReplicas(int replicas) {
		this.replicas = replicas;
		return this;
	}

	public int getMaxRetryAttempts() {
		return maxRetryAttempts;
	}

	public Configuration setMaxRetryAttempts(int maxRetryAttempts) {
		this.maxRetryAttempts = maxRetryAttempts;
		return this;
	}

	public long getTickPeriod() {
		return tickPeriod;
	}

	public Configuration setTickPeriod(long tickPeriod) {
		this.tickPeriod = tickPeriod;
		return this;
	}

	public long getServerDieTime() {
		return serverDieTime;
	}

	public Configuration setServerDieTime(int serverDieTime) {
		this.serverDieTime = serverDieTime;
		return this;
	}

	public long getServerDiscoveryTime() {
		return serverDiscoveryTime;
	}

	public Configuration setServerDiscoveryTime(long serverDiscoveryTime) {
		this.serverDiscoveryTime = serverDiscoveryTime;
		return this;
	}
}
