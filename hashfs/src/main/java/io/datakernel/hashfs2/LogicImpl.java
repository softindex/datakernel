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

import static io.datakernel.hashfs2.ServerInfo.ServerStatus.RUNNING;

public class LogicImpl implements Logic {
	private final long serverDeathTimeout;
	private final int maxReplicaQuantity;
	private final int minSafeReplicaQuantity;
	private final long approveWaitTime;
	private final ServerInfo myId;

	private final Commands commands;
	private final Hashing hashing;

	private final Map<String, FileInfo> files = new HashMap<>();
	private final Set<ServerInfo> servers = new HashSet<>();

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
	public void stop(CompletionCallback callback) {
		// TODO (arashev) assume no pending operations left
		callback.onComplete();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canUpload(String filePath) {
		FileInfo info = files.get(filePath);
		return info == null || info.isDeleted();
	}

	@Override
	public void onUploadStart(String filePath) {
		files.put(filePath, new FileInfo());
	}

	@Override
	public void onUploadComplete(String filePath) {
		commands.scheduleTemporaryFileDeletion(filePath, approveWaitTime);
	}

	@Override
	public void onUploadFailed(String filePath) {
		files.remove(filePath);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canApprove(String filePath) {
		FileInfo info = files.get(filePath);
		return info != null && !info.isDeleted() && !info.isReady();
	}

	@Override
	public void onApprove(String filePath) {
		files.get(filePath).onApprove(myId);
	}

	@Override
	public void onApproveCancel(String filePath) {
		files.remove(filePath);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canDownload(String filePath) {
		FileInfo info = files.get(filePath);
		return info != null && info.isReady();
	}

	@Override
	public void onDownloadStart(String filePath) {
		files.get(filePath).onOperationStart();
	}

	@Override
	public void onDownloadComplete(String filePath) {
		files.get(filePath).onOperationEnd();
	}

	@Override
	public void onDownloadFailed(String filePath) {
		files.get(filePath).onOperationEnd();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canDelete(String filePath) {
		FileInfo info = files.get(filePath);
		return info != null && info.canDelete();
	}

	@Override
	public void onDeletionStart(String filePath) {
		files.get(filePath).onDelete();
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
	public void onReplicationComplete(String filePath, ServerInfo server) {
		files.get(filePath).addReplica(server);
		files.get(filePath).onOperationEnd();
	}

	@Override
	public void onReplicationFailed(String filePath, ServerInfo server) {
		files.get(filePath).onOperationEnd();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void onShowAliveRequest(ResultCallback<Set<ServerInfo>> callback) {
		Set<ServerInfo> aliveServers = new HashSet<>();
		for (ServerInfo server : servers) {
			if (server.isAlive(serverDeathTimeout)) {
				aliveServers.add(server);
			}
		}
		callback.onResult(aliveServers);
	}

	@Override
	public void onShowAliveResponse(Set<ServerInfo> result, long timestamp) {
		for (ServerInfo server : servers) {
			if (result.contains(server)) {
				server.updateState(RUNNING, timestamp);
			}
		}
		for (ServerInfo server : result) {
			if (!servers.contains(server)) {
				servers.add(server);
				server.updateState(RUNNING, timestamp);
			}
		}
	}

	@Override
	public void onOfferRequest(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		for (String filePath : forDeletion) {
			FileInfo info = files.get(filePath);
			if (info.canDelete()) {
				info.onDelete();
				commands.delete(filePath);
			}
		}
		Set<String> required = new HashSet<>();
		for (String filePath : forUpload) {
			if (!files.containsKey(filePath)) {
				required.add(filePath);
			}
		}
		callback.onResult(required);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void update() {
		Map<ServerInfo, Set<String>> server2Offer = new HashMap<>();
		Map<ServerInfo, Set<String>> server2Delete = new HashMap<>();

		for (Iterator<String> it = files.keySet().iterator(); it.hasNext(); ) {

			String file = it.next();

			// getting servers-candidates for file handling
			Set<ServerInfo> alive = new HashSet<>();
			for (ServerInfo server : servers) {
				if (server.isAlive(serverDeathTimeout)) {
					alive.add(server);
				}
			}
			List<ServerInfo> candidates = hashing.sortServers(alive, file);
			candidates = candidates.subList(0, Math.min(candidates.size(), maxReplicaQuantity));
			Set<ServerInfo> currentReplicas = files.get(file).getReplicas();

			// removing servers that should not handle replica based on remote server default behavior
			for (ServerInfo server : currentReplicas) {
				if (!server.equals(myId) && !candidates.contains(server)) {
					currentReplicas.remove(server);
				}
			}

			// checking whether the current node should care about the file
			if (!candidates.contains(myId)) {
				if ((!files.get(file).isDeleted() && currentReplicas.size() > minSafeReplicaQuantity)
						|| (files.get(file).isDeleted())) {
					commands.delete(file);
					it.remove();
					continue;
				}
			} else {
				candidates.remove(myId);
			}

			// adding file to server2offerFiles map
			for (ServerInfo server : candidates) {
				Map<ServerInfo, Set<String>> workingMap = files.get(file).isDeleted() ? server2Delete : server2Offer;

				if (!currentReplicas.contains(server)) {
					Set<String> workingFiles = workingMap.get(server);
					if (workingFiles == null) {
						workingFiles = new HashSet<>();
						workingMap.put(server, workingFiles);
					}
					workingFiles.add(file);
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
					for (String filePath : forUpload) {
						if (!result.contains(filePath)) {
							FileInfo info = files.get(filePath);
							if (info != null && info.isReady()) {
								info.addReplica(server);
							}
						}
					}

					// trying to replicate welcomed files
					for (String file : result) {
						files.get(file).onOperationStart();
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
}
