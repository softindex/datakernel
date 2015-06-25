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

import java.util.HashSet;
import java.util.Set;

public class FileMetadata {

	// use set because replicas must be unique
	private final Set<ServerInfo> replicas = new HashSet<>();
	public FileStatus status;
	private long fileSize = 0;

	public FileMetadata(FileStatus status) {
		updateStatus(status);
	}

	private void updateStatus(FileStatus status) {
		this.status = status;
	}

	public void updateStatus(FileStatus status, long fileSize) {
		this.status = status;
		this.fileSize = fileSize;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void addReplica(ServerInfo serverInfo) {
		replicas.add(serverInfo);
	}

	public Set<ServerInfo> getReplicas() {
		return replicas;
	}

	public void clearReplicas() {
		replicas.clear();
	}

	public void removeReplica(ServerInfo serverInfo) {
		replicas.remove(serverInfo);
	}

	public FileBaseInfo getFileBaseInfo() {
		return new FileBaseInfo(status, fileSize);
	}

	public enum FileStatus {READY, IN_PROGRESS, TOMBSTONE}

	public static class FileBaseInfo {
		public final FileStatus fileStatus;
		public final long size;

		public FileBaseInfo(FileStatus fileStatus, long size) {
			this.fileStatus = fileStatus;
			this.size = size;
		}
	}

}
