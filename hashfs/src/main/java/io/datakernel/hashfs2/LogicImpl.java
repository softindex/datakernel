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

import java.util.*;

public class LogicImpl implements Logic {
	private static final long MAXIMUM_DIE_TIME = 10 * 1000;
	private static final int MAX_REPLICA_QUANTITY = 3;
	public static final int MINIMUM_SAFE_REPLICAS_QUANTITY = 1;

	private final Commands commands;
	private final Hashing hashing;
	private final ServerInfo myId;

	private final Map<String, FileInfo> files = new HashMap<>();
	private final Set<ServerInfo> servers = new HashSet<>();

	public LogicImpl(Hashing hashing, ServerInfo myId, Commands commands) {
		this.commands = commands;
		this.hashing = hashing;
		this.myId = myId;
	}

	@Override
	public void init(Set<ServerInfo> bootstrap) {
		servers.addAll(bootstrap);
		commands.scan(new ResultCallback<Set<String>>() {
			@Override
			public void onResult(Set<String> result) {
				for (String filePath : result) {
					files.put(filePath, new FileInfo(myId));
				}
			}

			@Override
			public void onException(Exception ignored) {
				// ignored
			}
		});
		commands.updateServerMap(bootstrap, new ResultCallback<Set<ServerInfo>>() {
			@Override
			public void onResult(Set<ServerInfo> result) {
				servers.addAll(result);
			}

			@Override
			public void onException(Exception ignored) {
				// ignored
			}
		});
		commands.postUpdate();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canUpload(String filePath) {
		return !files.containsKey(filePath) || files.get(filePath).isDeleted();
	}

	@Override
	public void onUploadStart(String filePath) {
		files.put(filePath, new FileInfo());
	}

	@Override
	public void onUploadComplete(String filePath) {
		commands.scheduleTemporaryFileDeletion(filePath);
	}

	@Override
	public void onUploadFailed(String filePath) {
		files.remove(filePath);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canApprove(String filePath) {
		return files.keySet().contains(filePath);
	}

	@Override
	public void onApprove(String filePath) {
		files.get(filePath).onApprove(myId);
	}

	@Override
	public void onApproveCancel(String filePath) {
		files.get(filePath).onApprove(myId);
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
		// ignored
	}

	@Override
	public void onDeleteComplete(String filePath) {
		files.get(filePath).onDelete();
	}

	@Override
	public void onDeleteFailed(String filePath) {
		// ignored
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void onReplicationComplete(String filePath, ServerInfo server) {
		files.get(filePath).addReplica(server);
	}

	@Override
	public void onReplicationFailed(String filePath, ServerInfo server) {
		// ignore
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void onShowAliveRequest(ResultCallback<Set<ServerInfo>> callback) {
		Set<ServerInfo> aliveServers = new HashSet<>();
		for (ServerInfo server : servers) {
			if (server.isAlive(MAXIMUM_DIE_TIME)) {
				aliveServers.add(server);
			}
		}
		callback.onResult(aliveServers);
	}

	@Override
	public void onOfferRequest(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		for (String filePath : forDeletion) {
			// TODO
			commands.delete(filePath);
		}

		Set<String> required = new HashSet<>();
		for (String filePath : forUpload) {
			if (!files.containsKey(filePath) || files.get(filePath).isDeleted()) {
				required.add(filePath);
			}
		}

		callback.onResult(required);
	}

	@Override
	public void update() {
		Map<ServerInfo, Set<String>> server2Offer = new HashMap<>();
		Map<ServerInfo, Set<String>> server2Delete = new HashMap<>();

		for (Iterator<String> it = files.keySet().iterator(); it.hasNext(); ) {

			String file = it.next();

			// getting servers-candidates for file handling
			List<ServerInfo> candidates = hashing.sortServers(servers, file);
			candidates = candidates.subList(0, Math.min(candidates.size(), MAX_REPLICA_QUANTITY));
			Set<ServerInfo> currentReplicas = files.get(file).getReplicas();

			// removing servers that should not handle replica based on remote server default behavior
			for (ServerInfo server : currentReplicas) {
				if (!server.equals(myId) && !candidates.contains(server)) {
					currentReplicas.remove(server);
				}
			}

			// checking whether the current node should care about the file
			if (!candidates.contains(myId)) {
				if ((!files.get(file).isDeleted() && currentReplicas.size() > MINIMUM_SAFE_REPLICAS_QUANTITY)
						|| (files.get(file).isDeleted())) {
					commands.delete(file);
					it.remove();
					continue;
				}
			}

			// adding file to server2offerFiles map
			for (ServerInfo server : candidates) {
				Map<ServerInfo, Set<String>> workingMap = files.get(file).isDeleted() ? server2Delete : server2Offer;
				if (!currentReplicas.contains(server)) {
					Set<String> currentServerFilesForOffer = workingMap.get(server);
					if (currentServerFilesForOffer == null) {
						currentServerFilesForOffer = new HashSet<>();
						workingMap.put(server, currentServerFilesForOffer);
					}
					currentServerFilesForOffer.add(file);
				}
			}
		}

		// Spreading files
		for (final ServerInfo server : server2Offer.keySet()) {
			Set<String> forDeletion = server2Delete.get(server);
			if (forDeletion == null) {
				forDeletion = new HashSet<>();
			}
			commands.offer(server, server2Offer.get(server), forDeletion, new ResultCallback<Set<String>>() {
				@Override
				public void onResult(Set<String> result) {
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
}
