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

import java.util.HashSet;
import java.util.Set;

import static io.datakernel.hashfs2.FileInfo.FileState.*;

final class FileInfo {
	private final Set<ServerInfo> replicas = new HashSet<>();
	private FileState state;
	private int pendingOperationsCounter;

	public FileInfo() {
		state = UPLOADING;
	}

	public FileInfo(ServerInfo myId) {
		state = READY;
		replicas.add(myId);
	}

	public void onApprove(ServerInfo holder) {
		replicas.add(holder);
		state = READY;
	}

	public void onDelete() {
		replicas.clear();
		state = TOMBSTONE;
	}

	public void onOperationStart() {
		pendingOperationsCounter++;
	}

	public void onOperationEnd() {
		pendingOperationsCounter--;
	}

	public boolean isReady() {
		return state == READY;
	}

	public boolean isDeleted() {
		return state == TOMBSTONE;
	}

	public boolean canDelete() {
		return pendingOperationsCounter == 0;
	}

	public void addReplica(ServerInfo server) {
		replicas.add(server);
	}

	public Set<ServerInfo> getReplicas() {
		return replicas;
	}

	enum FileState {
		UPLOADING, TOMBSTONE, READY
	}
}
