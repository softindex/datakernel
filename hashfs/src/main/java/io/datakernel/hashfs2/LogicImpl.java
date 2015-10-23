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

import io.datakernel.async.ResultCallback;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LogicImpl implements Logic {
	private static final long MAXIMUM_DIE_TIME = 10 * 1000;
	private final Hashing hashing;
	private final ServerInfo myId;
	private final Commands commands;

	private final Map<String, Set<ServerInfo>> files = new HashMap<>();
	private final Set<String> filesToBeDeleted = new HashSet<>();
	private final Set<ServerInfo> servers = new HashSet<>();

	public LogicImpl(Hashing hashing, ServerInfo serverInfo, Commands commands) {
		this.hashing = hashing;
		this.myId = serverInfo;
		this.commands = commands;
	}

	@Override
	public boolean canUpload(String filePath) {
		return !files.containsKey(filePath);
	}

	@Override
	public void onUploadStart(String filePath) {
		// ignored
	}

	@Override
	public void onUploadComplete(String filePath) {
		commands.scheduleTemporaryFileDeletion(filePath);
	}

	@Override
	public void onUploadFailed(String filePath) {
		// ignored
	}

	@Override
	public boolean canApprove(String filePath) {
		return true;
	}

	@Override
	public void onApprove(String filePath, boolean success) {
		if (success) {
			Set<ServerInfo> replicaHandlers = new HashSet<>();
			files.put(filePath, replicaHandlers);
			replicaHandlers.add(myId);
		}
	}

	@Override
	public boolean canDownload(String filePath) {
		return files.containsKey(filePath);
	}

	@Override
	public void onDownloadStart(String filePath) {
		// ignored
	}

	@Override
	public void onDownloadComplete(String filePath) {
		// ignored
	}

	@Override
	public void onDownloadFailed(String filePath) {
		// ignored
	}

	@Override
	public boolean canDelete(String filePath) {
		return files.containsKey(filePath);
	}

	@Override
	public void onDeletionStart(String filePath) {
		// ignored
	}

	@Override
	public void onDeleteComplete(String filePath) {
		files.remove(filePath);
		filesToBeDeleted.add(filePath);
	}

	@Override
	public void onDeleteFailed(String filePath) {
		// ignored
	}

	@Override
	public void onShowAlive(ResultCallback<Set<ServerInfo>> callback) {
		Set<ServerInfo> aliveServers = new HashSet<>();
		for (ServerInfo server : servers) {
			if (server.isAlive(MAXIMUM_DIE_TIME)) {
				aliveServers.add(server);
			}
		}
		callback.onResult(aliveServers);
	}

	@Override
	public void onOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
		for (String filePath : forDeletion) {
			commands.delete(filePath);
		}
		Set<String> required = new HashSet<>();
		for (String filePath : forUpload) {
			if (!files.containsKey(filePath)) {
				required.add(filePath);
			}
		}
		result.onResult(required);
	}

	@Override
	public void onReplicationComplete(String filePath, ServerInfo server) {
		files.get(filePath).add(server);
	}

	@Override
	public void onReplicationFailed(String filePath, ServerInfo server) {
		// ignore
	}

	@Override
	public void update() {
		// TODO (arashev)
	}

	@Override
	public void init() {
		commands.scan(new ResultCallback<Set<String>>() {
			@Override
			public void onResult(Set<String> result) {
				for (String filePath : result) {
					Set<ServerInfo> replicas = new HashSet<>();
					replicas.add(myId);
					files.put(filePath, replicas);
				}
			}

			@Override
			public void onException(Exception ignored) {
				// ignored
			}
		});

		commands.updateServerMap(servers, new ResultCallback<Set<ServerInfo>>() {
			@Override
			public void onResult(Set<ServerInfo> result) {
				servers.addAll(result);
			}

			@Override
			public void onException(Exception ignored) {
				// ignored
			}
		});
		commands.updateSystem();
	}
}
