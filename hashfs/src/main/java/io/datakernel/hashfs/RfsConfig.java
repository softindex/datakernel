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

import io.datakernel.net.SocketSettings;

import static io.datakernel.net.SocketSettings.defaultSocketSettings;

public final class RfsConfig {
	public static final int SECOND = 1000;

	public static RfsConfig getDefaultConfig() {
		return new RfsConfig();
	}

	// LOGIC
	private long serverDeathTimeout = 11 * SECOND;
	private long approveWaitTime = 10 * SECOND;
	private int maxReplicaQuantity = 3;
	private int minSafeReplicasQuantity = 1;

	// PROTOCOL
	private int minChunkSize = 64 * 1024;
	private int maxChunkSize = 128 * 1024;
	private int deserializerBufferSize = 10;
	private int connectTimeout = 0;
	private int serializerBufferSize = 256 * 1024;
	private int serializerMaxMessageSize = 256 * (1 << 20);
	private int serializerFlushDelayMillis = 0;
	private SocketSettings socketSettings = defaultSocketSettings();

	// SERVER
	private long systemUpdateTimeout = 10 * SECOND;
	private long mapUpdateTimeout = 10 * SECOND;

	// CLIENT
	private int maxRetryAttempts = 3;
	private long baseRetryTimeout = 100;

	public int getMaxRetryAttempts() {
		return maxRetryAttempts;
	}

	public void setMaxRetryAttempts(int maxRetryAttempts) {
		this.maxRetryAttempts = maxRetryAttempts;
	}

	public long getBaseRetryTimeout() {
		return baseRetryTimeout;
	}

	public void setBaseRetryTimeout(long baseRetryTimeout) {
		this.baseRetryTimeout = baseRetryTimeout;
	}

	public long getSystemUpdateTimeout() {
		return systemUpdateTimeout;
	}

	public void setSystemUpdateTimeout(long systemUpdateTimeout) {
		this.systemUpdateTimeout = systemUpdateTimeout;
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	public void setSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
	}

	public int getSerializerFlushDelayMillis() {
		return serializerFlushDelayMillis;
	}

	public void setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
		this.serializerFlushDelayMillis = serializerFlushDelayMillis;
	}

	public int getSerializerMaxMessageSize() {
		return serializerMaxMessageSize;
	}

	public void setSerializerMaxMessageSize(int serializerMaxMessageSize) {
		this.serializerMaxMessageSize = serializerMaxMessageSize;
	}

	public int getSerializerBufferSize() {
		return serializerBufferSize;
	}

	public void setSerializerBufferSize(int serializerBufferSize) {
		this.serializerBufferSize = serializerBufferSize;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public int getDeserializerBufferSize() {
		return deserializerBufferSize;
	}

	public void setDeserializerBufferSize(int deserializerBufferSize) {
		this.deserializerBufferSize = deserializerBufferSize;
	}

	public int getMaxChunkSize() {
		return maxChunkSize;
	}

	public void setMaxChunkSize(int maxChunkSize) {
		this.maxChunkSize = maxChunkSize;
	}

	public int getMinChunkSize() {
		return minChunkSize;
	}

	public void setMinChunkSize(int minChunkSize) {
		this.minChunkSize = minChunkSize;
	}

	public long getApproveWaitTime() {
		return approveWaitTime;
	}

	public void setApproveWaitTime(long approveWaitTime) {
		this.approveWaitTime = approveWaitTime;
	}

	public int getMinSafeReplicasQuantity() {
		return minSafeReplicasQuantity;
	}

	public void setMinSafeReplicasQuantity(int minSafeReplicasQuantity) {
		this.minSafeReplicasQuantity = minSafeReplicasQuantity;
	}

	public int getMaxReplicaQuantity() {
		return maxReplicaQuantity;
	}

	public void setMaxReplicaQuantity(int maxReplicaQuantity) {
		this.maxReplicaQuantity = maxReplicaQuantity;
	}

	public long getServerDeathTimeout() {
		return serverDeathTimeout;
	}

	public void setServerDeathTimeout(long serverDeathTimeout) {
		this.serverDeathTimeout = serverDeathTimeout;
	}

	public long getMapUpdateTimeout() {
		return mapUpdateTimeout;
	}

	public void setMapUpdateTimeout(long mapUpdateTimeout) {
		this.mapUpdateTimeout = mapUpdateTimeout;
	}
}