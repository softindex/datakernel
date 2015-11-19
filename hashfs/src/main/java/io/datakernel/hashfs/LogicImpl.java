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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;

import java.util.*;
import java.util.Map.Entry;

import static io.datakernel.hashfs.LogicImpl.FileState.*;

final class LogicImpl implements Logic {
	public static class Builder {
		private final ServerInfo myId;
		private HashingStrategy hashing = DEFAULT_HASHING_STRATEGY;
		private final Set<ServerInfo> bootstrap;

		private long serverDeathTimeout = DEFAULT_SERVER_DEATH_TIMEOUT;
		private long approveWaitTime = DEFAULT_APPROVE_WAIT_TIME;
		private int maxReplicaQuantity = DEFAULT_MAX_REPLICA_QUANTITY;
		private int minSafeReplicaQuantity = DEFAULT_MIN_REPLICA_QUANTITY;

		private Builder(ServerInfo myId, Set<ServerInfo> bootstrap) {
			this.myId = myId;
			this.bootstrap = bootstrap;
		}

		public Builder setServerDeathTimeout(long serverDeathTimeout) {
			this.serverDeathTimeout = serverDeathTimeout;
			return this;
		}

		public Builder setApproveWaitTime(long approveWaitTime) {
			this.approveWaitTime = approveWaitTime;
			return this;
		}

		public Builder setMaxReplicaQuantity(int maxReplicaQuantity) {
			this.maxReplicaQuantity = maxReplicaQuantity;
			return this;
		}

		public Builder setMinSafeReplicaQuantity(int minSafeReplicaQuantity) {
			this.minSafeReplicaQuantity = minSafeReplicaQuantity;
			return this;
		}

		public Builder setHashing(HashingStrategy hashing) {
			this.hashing = hashing;
			return this;
		}

		public LogicImpl build() {
			return new LogicImpl(hashing, myId, bootstrap, serverDeathTimeout, maxReplicaQuantity,
					minSafeReplicaQuantity, approveWaitTime);
		}
	}

	public static final long DEFAULT_SERVER_DEATH_TIMEOUT = 11 * 1000;
	public static final long DEFAULT_APPROVE_WAIT_TIME = 10 * 1000;
	public static final int DEFAULT_MAX_REPLICA_QUANTITY = 3;
	public static final int DEFAULT_MIN_REPLICA_QUANTITY = 1;
	public static final HashingStrategy DEFAULT_HASHING_STRATEGY = new RendezvousHashing();

	private final long serverDeathTimeout;
	private final long approveWaitTime;
	private final int maxReplicaQuantity;
	private final int minSafeReplicaQuantity;
	private final ServerInfo myId;

	private final HashingStrategy hashing;
	private Commands commands;

	private final Map<String, FileInfo> files = new HashMap<>();
	private final Set<ServerInfo> servers = new HashSet<>();

	private CompletionCallback onStopCallback;

	private LogicImpl(HashingStrategy hashing, ServerInfo myId, Set<ServerInfo> bootstrap,
	                  long serverDeathTimeout, int maxReplicaQuantity, int minSafeReplicaQuantity, long approveWaitTime) {
		this.hashing = hashing;
		this.myId = myId;
		this.serverDeathTimeout = serverDeathTimeout;
		this.maxReplicaQuantity = maxReplicaQuantity;
		this.minSafeReplicaQuantity = minSafeReplicaQuantity;
		this.approveWaitTime = approveWaitTime;
		this.servers.addAll(bootstrap);
	}

	public static LogicImpl createInstance(ServerInfo myId, Set<ServerInfo> bootstrap) {
		return buildInstance(myId, bootstrap).build();
	}

	public static Builder buildInstance(ServerInfo myId, Set<ServerInfo> bootstrap) {
		return new Builder(myId, bootstrap);
	}

	@Override
	public void wire(Commands commands) {
		this.commands = commands;
	}

	@Override
	public void start(final CompletionCallback callback) {
		commands.scan(new ForwardingResultCallback<Set<String>>(callback) {
			@Override
			public void onResult(Set<String> result) {
				for (String s : result) {
					files.put(s, new FileInfo(myId));
				}
				commands.updateServerMap(servers);
				commands.scheduleUpdate();
				callback.onComplete();
			}
		});
	}

	@Override
	public void stop(CompletionCallback callback) {
		for (Entry<String, FileInfo> file : files.entrySet()) {
			FileInfo info = file.getValue();
			if (info.pendingOperationsCounter != 0) {
				onStopCallback = callback;
				return;
			}
		}
		files.clear();
		servers.clear();
		callback.onComplete();
	}

	@Override
	public boolean canUpload(String fileName) {
		FileInfo info = files.get(fileName);
		return info == null || info.state == TOMBSTONE;
	}

	@Override
	public void onUploadStart(String fileName) {
		FileInfo info = new FileInfo();
		info.pendingOperationsCounter++;
		info.state = UPLOADING;
		files.put(fileName, info);
	}

	@Override
	public void onUploadComplete(String fileName) {
		commands.scheduleCommitCancel(fileName, approveWaitTime);
	}

	@Override
	public void onUploadFailed(String fileName) {
		onApproveCancel(fileName);
	}

	@Override
	public boolean canApprove(String fileName) {
		// assuming commit message won't be send unless upload finished
		FileInfo info = files.get(fileName);
		return info != null && info.state == UPLOADING;
	}

	@Override
	public void onApprove(String fileName) {
		FileInfo info = files.get(fileName);
		info.state = READY;
		info.pendingOperationsCounter--;
		onOperationFinished();
	}

	@Override
	public void onApproveCancel(String fileName) {
		files.remove(fileName);
		onOperationFinished();
	}

	@Override
	public boolean canDownload(String fileName) {
		FileInfo info = files.get(fileName);
		return info != null && info.state == READY;
	}

	@Override
	public void onDownloadStart(String fileName) {
		FileInfo info = files.get(fileName);
		info.pendingOperationsCounter++;
	}

	@Override
	public void onDownloadComplete(String fileName) {
		FileInfo info = files.get(fileName);
		info.pendingOperationsCounter--;
		onOperationFinished();
	}

	@Override
	public void onDownloadFailed(String fileName) {
		onDownloadComplete(fileName);
	}

	@Override
	public boolean canDelete(String fileName) {
		FileInfo info = files.get(fileName);
		return info != null && info.pendingOperationsCounter == 0;
	}

	@Override
	public void onDeletionStart(String fileName) {
		FileInfo info = files.get(fileName);
		info.state = TOMBSTONE;
		info.replicas.clear();
	}

	@Override
	public void onDeleteComplete(String fileName) {
		// ignored
	}

	@Override
	public void onDeleteFailed(String fileName) {
		// ignored
	}

	@Override
	public void onReplicationStart(String fileName) {
		FileInfo info = files.get(fileName);
		info.pendingOperationsCounter++;
	}

	@Override
	public void onReplicationComplete(ServerInfo server, String fileName) {
		FileInfo info = files.get(fileName);
		info.replicas.add(server);
		info.pendingOperationsCounter--;
	}

	@Override
	public void onReplicationFailed(ServerInfo server, String fileName) {
		FileInfo info = files.get(fileName);
		info.pendingOperationsCounter--;
	}

	@Override
	public void onShowAliveRequest(long timestamp, ResultCallback<Set<ServerInfo>> callback) {
		Set<ServerInfo> aliveServers = new HashSet<>();
		for (ServerInfo server : servers) {
			if (server.isAlive(timestamp - serverDeathTimeout)) {
				aliveServers.add(server);
			}
		}
		aliveServers.add(myId);
		callback.onResult(aliveServers);
	}

	@Override
	public void onShowAliveResponse(long timestamp, Set<ServerInfo> result) {
		for (ServerInfo server : servers) {
			if (result.contains(server)) {
				server.updateState(timestamp);
			}
		}
		for (ServerInfo server : result) {
			if (!servers.contains(server)) {
				servers.add(server);
				server.updateState(timestamp);
			}
		}
		for (FileInfo info : files.values()) {
			for (Iterator<ServerInfo> it = info.replicas.iterator(); it.hasNext(); ) {
				ServerInfo server = it.next();
				if (!result.contains(server)) {
					it.remove();
				}
			}
		}
	}

	@Override
	public void onOfferRequest(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		for (String fileName : forDeletion) {
			FileInfo info = files.get(fileName);
			if (info.pendingOperationsCounter == 0) {
				info.replicas.clear();
				info.state = TOMBSTONE;
				commands.delete(fileName);
			}
		}
		Set<String> required = new HashSet<>();
		for (String fileName : forUpload) {
			if (!files.containsKey(fileName) || files.get(fileName).state != READY) {
				required.add(fileName);
			}
		}
		callback.onResult(required);
	}

	@Override
	public void update(long timestamp) {
		Map<ServerInfo, Set<String>> server2Offer = new HashMap<>();
		Map<ServerInfo, Set<String>> server2Delete = new HashMap<>();

		for (Iterator<Entry<String, FileInfo>> it = files.entrySet().iterator(); it.hasNext(); ) {

			Entry<String, FileInfo> entry = it.next();
			String file = entry.getKey();
			FileInfo info = entry.getValue();

			// if file state is not consistent yet omit it
			if (info.state == UPLOADING) {
				continue;
			}

			// getting servers-candidates for file handling
			Set<ServerInfo> alive = new HashSet<>();
			for (ServerInfo server : servers) {
				if (server.isAlive(timestamp - serverDeathTimeout)) {
					alive.add(server);
				}
			}
			List<ServerInfo> candidates = hashing.sortServers(file, alive);
			candidates = candidates.subList(0, Math.min(candidates.size(), maxReplicaQuantity));
			Set<ServerInfo> currentReplicas = files.get(file).replicas;

			// removing servers that should not handle replica based on remote server default behavior
			for (ServerInfo server : currentReplicas) {
				if (!server.equals(myId) && !candidates.contains(server)) {
					currentReplicas.remove(server);
				}
			}

			// checking if this node should care about the file
			if (!candidates.contains(myId)) {
				if ((info.state == READY) && currentReplicas.size() > minSafeReplicaQuantity) {
					commands.delete(file);
					info.replicas.clear();
					it.remove();
					continue;
				} else if (info.state == TOMBSTONE) {
					info.replicas.clear();
					it.remove();
					continue;
				}
			} else {
				candidates.remove(myId);
			}

			// adding file to server2offerFiles map
			for (ServerInfo server : candidates) {
				Map<ServerInfo, Set<String>> map = (info.state == TOMBSTONE) ? server2Delete : server2Offer;
				if (!currentReplicas.contains(server)) {
					Set<String> set = map.get(server);
					if (set == null) {
						set = new HashSet<>();
						map.put(server, set);
					}
					set.add(file);
				}
			}
		}

		// Spreading files
		Set<ServerInfo> directions = new HashSet<>();
		directions.addAll(server2Offer.keySet());
		directions.addAll(server2Delete.keySet());
		for (final ServerInfo server : directions) {
			final Set<String> forUpload = server2Offer.get(server) == null ? new HashSet<String>() : server2Offer.get(server);
			Set<String> forDeletion = server2Delete.get(server) == null ? new HashSet<String>() : server2Delete.get(server);

			commands.offer(server, forUpload, forDeletion, new ResultCallback<Set<String>>() {
				@Override
				public void onResult(Set<String> result) {
					// assume that all of suggested files being rejected exist at remote server
					for (String file : forUpload) {
						if (!result.contains(file)) {
							FileInfo info = files.get(file);
							if (info != null && info.state == READY) {
								info.replicas.add(server);
							}
						}
					}

					// trying to replicate welcomed files
					for (String file : result) {
						commands.replicate(server, file);
					}
				}

				@Override
				public void onException(Exception ignored) {
					// nothing to do: either remote server shut down or internal Exception
				}
			});
		}
		commands.scheduleUpdate();
	}

	private void onOperationFinished() {
		if (onStopCallback != null) {
			for (Entry<String, FileInfo> file : files.entrySet()) {
				FileInfo info = file.getValue();
				if (info.pendingOperationsCounter != 0) {
					return;
				}
			}
			files.clear();
			servers.clear();
			onStopCallback.onComplete();
		}
	}

	private final class FileInfo {
		final Set<ServerInfo> replicas = new HashSet<>();
		FileState state;
		int pendingOperationsCounter;

		public FileInfo() {
		}

		public FileInfo(ServerInfo myId) {
			state = READY;
			replicas.add(myId);
		}
	}

	enum FileState {
		UPLOADING, TOMBSTONE, READY
	}
}