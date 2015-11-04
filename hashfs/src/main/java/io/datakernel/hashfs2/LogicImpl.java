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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;

import java.util.*;
import java.util.Map.Entry;

import static io.datakernel.hashfs2.LogicImpl.FileState.*;

public final class LogicImpl implements Logic {
	private final long serverDeathTimeout;
	private final long approveWaitTime;
	private final int maxReplicaQuantity;
	private final int minSafeReplicaQuantity;
	private final ServerInfo myId;

	private final Commands commands;
	private final Hashing hashing;

	private final Map<String, FileInfo> files = new HashMap<>();
	private final Set<ServerInfo> servers = new HashSet<>();

	private CompletionCallback onStopCallback;

	public LogicImpl(Commands commands, Hashing hashing, ServerInfo myId, Set<ServerInfo> bootstrap,
	                 long serverDeathTimeout, int maxReplicaQuantity, int minSafeReplicaQuantity, long approveWaitTime) {
		this.commands = commands;
		this.hashing = hashing;
		this.myId = myId;
		this.serverDeathTimeout = serverDeathTimeout;
		this.maxReplicaQuantity = maxReplicaQuantity;
		this.minSafeReplicaQuantity = minSafeReplicaQuantity;
		this.approveWaitTime = approveWaitTime;
		this.servers.addAll(bootstrap);
	}

	@Override
	public NioEventloop getNioEventloop() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void start(final CompletionCallback callback) {
		commands.scan(new ResultCallback<Set<String>>() {
			@Override
			public void onResult(Set<String> result) {
				for (String filePath : result) {
					files.put(filePath, new FileInfo(myId));
				}
				commands.updateServerMap(servers);
				commands.postUpdate();
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void stop(final CompletionCallback callback) {
		onStopCallback = callback;
		onOperationFinished();
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

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canUpload(String filePath) {
		FileInfo info = files.get(filePath);
		return info == null || info.state == TOMBSTONE;
	}

	@Override
	public void onUploadStart(String filePath) {
		FileInfo info = new FileInfo();
		info.pendingOperationsCounter++;
		info.state = UPLOADING;
		files.put(filePath, info);
	}

	@Override
	public void onUploadComplete(String filePath) {
		commands.scheduleTemporaryFileDeletion(filePath, approveWaitTime);
	}

	@Override
	public void onUploadFailed(String filePath) {
		onApproveCancel(filePath);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canApprove(String filePath) {
		// assuming commit message won't be send unless upload finished
		FileInfo info = files.get(filePath);
		return info != null && info.state == UPLOADING;
	}

	@Override
	public void onApprove(String filePath) {
		FileInfo info = files.get(filePath);
		info.state = READY;
		info.pendingOperationsCounter--;
		onOperationFinished();
	}

	@Override
	public void onApproveCancel(String filePath) {
		files.remove(filePath);
		onOperationFinished();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canDownload(String filePath) {
		FileInfo info = files.get(filePath);
		return info != null && info.state == READY;
	}

	@Override
	public void onDownloadStart(String filePath) {
		FileInfo info = files.get(filePath);
		info.pendingOperationsCounter++;
	}

	@Override
	public void onDownloadComplete(String filePath) {
		FileInfo info = files.get(filePath);
		info.pendingOperationsCounter--;
	}

	@Override
	public void onDownloadFailed(String filePath) {
		onDownloadComplete(filePath);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canDelete(String filePath) {
		FileInfo info = files.get(filePath);
		return info != null && info.pendingOperationsCounter == 0;
	}

	@Override
	public void onDeletionStart(String filePath) {
		FileInfo info = files.get(filePath);
		info.state = TOMBSTONE;
		info.replicas.clear();
	}

	@Override
	public void onDeleteComplete(String filePath) {
		// ignored
	}

	@Override
	public void onDeleteFailed(String filePath) {
		// ignored
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void onReplicationStart(String filePath) {
		FileInfo info = files.get(filePath);
		info.pendingOperationsCounter++;
	}

	@Override
	public void onReplicationComplete(String filePath, ServerInfo server) {
		FileInfo info = files.get(filePath);
		info.replicas.add(server);
		info.pendingOperationsCounter--;
	}

	@Override
	public void onReplicationFailed(String filePath, ServerInfo server) {
		FileInfo info = files.get(filePath);
		info.pendingOperationsCounter--;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void onShowAliveRequest(long timestamp, ResultCallback<Set<ServerInfo>> callback) {
		Set<ServerInfo> aliveServers = new HashSet<>();
		for (ServerInfo server : servers) {
			if (server.isAlive(timestamp - serverDeathTimeout)) {
				aliveServers.add(server);
			}
		}
		callback.onResult(aliveServers);
	}

	@Override
	public void onShowAliveResponse(Set<ServerInfo> result, long timestamp) {
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
	}

	@Override
	public void onOfferRequest(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		for (String filePath : forDeletion) {
			FileInfo info = files.get(filePath);
			if (info.pendingOperationsCounter == 0) {
				info.replicas.clear();
				info.state = TOMBSTONE;
				commands.delete(filePath);
			}
		}
		Set<String> required = new HashSet<>();
		for (String filePath : forUpload) {
			if (!files.containsKey(filePath) || files.get(filePath).state != READY) {
				required.add(filePath);
			}
		}
		callback.onResult(required);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
			List<ServerInfo> candidates = hashing.sortServers(alive, file);
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
						commands.replicate(file, server);
					}
				}

				@Override
				public void onException(Exception ignored) {
					// nothing to do: either remote server shut down or internal Exception
				}
			});
		}
		commands.postUpdate();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
