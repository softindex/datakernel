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

public class Config {
	public static final Config defaultConfig = new Config();

	// FILESYSTEM
	private String inProgressExtension = ".partial";
	private String tmpDirectoryName = "tmp";
	private int fsBufferSize = 256 * 1024;
	// LOGIC
	private long serverDeathTimeout = 10 * 1000;
	private int maxReplicaQuantity = 3;
	private int minSafeReplicasQuantity = 1;
	// PROTOCOL

	// SERVER

	public Config setupFs(String inProgressExtension, String tmpDirectoryName, int bufferSize) {
		this.inProgressExtension = inProgressExtension;
		this.tmpDirectoryName = tmpDirectoryName;
		this.fsBufferSize = bufferSize;
		return this;
	}

	public Config setupLogic(long serverDeathTimeout, int maxReplicaQuantity, int minSafeReplicasQuantity) {
		this.serverDeathTimeout = serverDeathTimeout;
		this.maxReplicaQuantity = maxReplicaQuantity;
		this.minSafeReplicasQuantity = minSafeReplicasQuantity;
		return this;
	}

	public String getInProgressExtension() {
		return inProgressExtension;
	}

	public String getTmpDirectoryName() {
		return tmpDirectoryName;
	}

	public int getFsBufferSize() {
		return fsBufferSize;
	}

	public long getServerDeathTimeout() {
		return serverDeathTimeout;
	}

	public int getMaxReplicaQuantity() {
		return maxReplicaQuantity;
	}

	public int getMinSafeReplicasQuantity() {
		return minSafeReplicasQuantity;
	}

}
