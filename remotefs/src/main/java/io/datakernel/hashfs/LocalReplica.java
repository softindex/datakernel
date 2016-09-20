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

import io.datakernel.FileManager;
import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.stream.file.StreamFileReader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.async.AsyncCallbacks.ignoreResultCallback;
import static io.datakernel.hashfs.LocalReplica.FileState.*;
import static java.lang.Math.min;

public final class LocalReplica implements EventloopService {
	private final Eventloop eventloop;
	private final FileManager fileManager;
	private final HashFsClient client;

	private final Replica myId;

	private final Map<String, FileInfo> files = new HashMap<>();
	private final Set<Replica> replicas = new HashSet<>();

	private HashingStrategy hashing = new RendezvousHashing();
	private long serverDeathTimeout = 10 * 1000L;
	private int maxReplicaQuantity = 3;
	private int minSafeReplicaQuantity = 1;
	private long systemUpdateTimeout = 5 * 1000L;
	private long mapUpdateTimeout = 10 * 1000L;

	private CompletionCallback onStopCallback;

	// region creators & builder setters
	private LocalReplica(Eventloop eventloop, ExecutorService executor, Path storagePath,
	                     List<Replica> bootstrap, Replica myId) {
		this.eventloop = eventloop;
		this.fileManager = FileManager.create(eventloop, executor, storagePath);
		this.client = HashFsClient.create(eventloop, bootstrap);
		replicas.addAll(bootstrap);
		this.myId = myId;
	}

	public static LocalReplica create(Eventloop eventloop, ExecutorService executor, Path storagePath, List<Replica> bootstrap, Replica myId) {return new LocalReplica(eventloop, executor, storagePath, bootstrap, myId);}

	public LocalReplica withServerDeathTimeout(long serverDeathTimeout) {
		this.serverDeathTimeout = serverDeathTimeout;
		return this;
	}

	public LocalReplica withMaxReplicaQuantity(int maxReplicaQuantity) {
		this.maxReplicaQuantity = maxReplicaQuantity;
		return this;
	}

	public LocalReplica withMinSafeReplicaQuantity(int minSafeReplicaQuantity) {
		this.minSafeReplicaQuantity = minSafeReplicaQuantity;
		return this;
	}

	public LocalReplica withSystemUpdateTimeout(long systemUpdateTimeout) {
		this.systemUpdateTimeout = systemUpdateTimeout;
		return this;
	}

	public LocalReplica withMapUpdateTimeout(long mapUpdateTimeout) {
		this.mapUpdateTimeout = mapUpdateTimeout;
		return this;
	}

	public LocalReplica withHashing(HashingStrategy hashing) {
		this.hashing = hashing;
		return this;
	}
	// endregion

	public Replica getMyId() {
		return myId;
	}

	FileManager getFileManager() {
		return fileManager;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		try {
			List<String> result = fileManager.scan();
			for (String file : result) {
				files.put(file, new FileInfo(myId));
			}
			for (Replica info : replicas) {
				info.updateState(eventloop.currentTimeMillis());
			}
			scheduleUpdateServersMap();
			scheduleUpdate();
			callback.onComplete();
		} catch (IOException e) {
			callback.onException(e);
		}
	}

	@Override
	public void stop(CompletionCallback callback) {
		this.onStopCallback = callback;
		onOperationFinished();
	}

	// upload cycle
	boolean canUpload(String file) {
		FileInfo info = files.get(file);
		return info == null || info.state == TOMBSTONE;
	}

	void onUploadStart(String file) {
		FileInfo info = new FileInfo();
		info.pendingOperationsCounter++;
		info.state = UPLOADING;
		files.put(file, info);
	}

	void onUploadComplete(String file) {
		FileInfo info = files.get(file);
		info.state = READY;
		info.pendingOperationsCounter--;
		onOperationFinished();
	}

	void onUploadFailed(String file) {
		files.remove(file);
		onOperationFinished();
	}

	// download cycle
	boolean canDownload(String file) {
		FileInfo info = files.get(file);
		return info != null && info.state == READY;
	}

	void onDownloadStart(String file) {
		FileInfo info = files.get(file);
		info.pendingOperationsCounter++;
	}

	void onDownloadComplete(String file) {
		FileInfo info = files.get(file);
		info.pendingOperationsCounter--;
		onOperationFinished();
	}

	void onDownloadFailed(String file) {
		onDownloadComplete(file);
	}

	// delete cycle
	boolean canDelete(String file) {
		FileInfo info = files.get(file);
		return info != null && info.pendingOperationsCounter == 0;
	}

	void onDeletionStart(String file) {
		FileInfo info = files.get(file);
		info.state = TOMBSTONE;
		info.replicas.clear();
	}

	void onDeleteComplete(String file) {
		// ignored
	}

	void onDeleteFailed(String file) {
		// ignored
	}

	// replication cycle
	private void onReplicationStart(String file) {
		FileInfo info = files.get(file);
		info.pendingOperationsCounter++;
	}

	private void onReplicationComplete(Replica server, String file) {
		FileInfo info = files.get(file);
		info.replicas.add(server);
		info.pendingOperationsCounter--;
	}

	private void onReplicationFailed(String file) {
		FileInfo info = files.get(file);
		info.pendingOperationsCounter--;
	}

	private void replicate(final Replica server, final String file) {
		onReplicationStart(file);
		fileManager.get(file, 0, new ResultCallback<StreamFileReader>() {
			@Override
			public void onResult(StreamFileReader reader) {
				client.makeReplica(server, file, reader, new ForwardingCompletionCallback(this) {
					@Override
					public void onComplete() {
						onReplicationComplete(server, file);
					}
				});
			}

			@Override
			public void onException(Exception e) {
				onReplicationFailed(file);
			}
		});
	}

	// list cycle
	public void getList(ResultCallback<List<String>> callback) {
		List<String> list = new ArrayList<>();
		for (Entry<String, FileInfo> entry : files.entrySet()) {
			if (entry.getValue().state == READY) {
				list.add(entry.getKey());
			}
		}
		callback.onResult(list);
	}

	// utils
	void showAlive(long timestamp, ResultCallback<Set<Replica>> callback) {
		Set<Replica> aliveServers = new HashSet<>();
		for (Replica server : replicas) {
			if (server.isAlive(timestamp - serverDeathTimeout)) {
				aliveServers.add(server);
			}
		}
		aliveServers.add(myId);
		callback.onResult(aliveServers);
	}

	private void onShowAliveResponse(long timestamp, Set<Replica> alive) {
		for (Replica server : replicas) {
			if (alive.contains(server)) {
				server.updateState(timestamp);
			}
		}
		for (Replica server : alive) {
			if (!replicas.contains(server)) {
				replicas.add(server);
				server.updateState(timestamp);
			}
		}
	}

	void onAnnounce(List<String> forUpload, List<String> forDeletion, ResultCallback<List<String>> callback) {
		for (final String file : forDeletion) {
			if (canDelete(file)) {
				onDeletionStart(file);
				fileManager.delete(file, new SimpleCompletionCallback() {
					@Override
					protected void onCompleteOrException() {
						onDeleteComplete(file);
					}
				});
			}
		}
		List<String> required = new ArrayList<>();
		for (String file : forUpload) {
			FileInfo info = files.get(file);
			// all the files we have no info about + those who haven't fully uploaded yet
			if (info == null || info.state != READY) {
				required.add(file);
			}
		}
		callback.onResult(required);
	}

	private void scheduleUpdate() {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + systemUpdateTimeout, new Runnable() {
			@Override
			public void run() {
				update(eventloop.currentTimeMillis());
				scheduleUpdate();
			}
		});
	}

	private void update(long timestamp) {
		Map<Replica, List<String>> server2Offer = new HashMap<>();
		Map<Replica, List<String>> server2Delete = new HashMap<>();

		for (Iterator<Entry<String, FileInfo>> it = files.entrySet().iterator(); it.hasNext(); ) {

			Entry<String, FileInfo> entry = it.next();
			String file = entry.getKey();
			FileInfo info = entry.getValue();

			// if file state is not consistent yet omit it
			if (info.state == UPLOADING) {
				continue;
			}

			// getting servers-candidates for file handling
			List<Replica> alive = new ArrayList<>();
			for (Replica replica : replicas) {
				if (replica.isAlive(timestamp - serverDeathTimeout)) {
					alive.add(replica);
				}
			}
			List<Replica> candidates = hashing.sortReplicas(file, alive);
			candidates = candidates.subList(0, min(candidates.size(), maxReplicaQuantity));
			Set<Replica> fileReplicas = files.get(file).replicas;

			// removing servers that should not handle replica based on remote server default behavior
			for (Replica server : fileReplicas) {
				if (!server.equals(myId) && !candidates.contains(server)) {
					fileReplicas.remove(server);
				}
			}

			// checking if this node should care about the file
			if (!candidates.contains(myId)) {
				if ((info.state == READY) && fileReplicas.size() > minSafeReplicaQuantity) {
					fileManager.delete(file, ignoreCompletionCallback());
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
			for (Replica server : candidates) {
				Map<Replica, List<String>> map = (info.state == TOMBSTONE) ? server2Delete : server2Offer;
				if (!fileReplicas.contains(server)) {
					List<String> set = map.get(server);
					if (set == null) {
						set = new ArrayList<>();
						map.put(server, set);
					}
					set.add(file);
				}
			}
		}

		// Spreading files
		List<Replica> directions = new ArrayList<>();
		directions.addAll(server2Offer.keySet());
		directions.addAll(server2Delete.keySet());
		for (final Replica replica : directions) {
			final List<String> forUpload = server2Offer.containsKey(replica) ? server2Offer.get(replica) : new ArrayList<String>();
			List<String> forDeletion = server2Delete.containsKey(replica) ? server2Delete.get(replica) : new ArrayList<String>();
			client.announce(replica, forUpload, forDeletion, new ForwardingResultCallback<List<String>>(ignoreResultCallback()) {
				@Override
				public void onResult(List<String> result) {
					// we assume that all of the suggested files that has been rejected exist at remote server
					for (String file : forUpload) {
						if (!result.contains(file)) {
							FileInfo info = files.get(file);
							if (info != null && info.state == READY) {
								info.replicas.add(replica);
							}
						}
					}
					// trying to replicate welcomed files
					for (String file : result) {
						FileInfo fileInfo = files.get(file);
						if (fileInfo != null && fileInfo.state != TOMBSTONE) {
							replicate(replica, file);
						}
					}
				}
			});
		}
	}

	private void onOperationFinished() {
		if (onStopCallback != null) {
			for (Entry<String, FileInfo> file : files.entrySet()) {
				if (file.getValue().pendingOperationsCounter != 0) {
					return;
				}
			}
			files.clear();
			replicas.clear();
			onStopCallback.onComplete();
		}
	}

	private void scheduleUpdateServersMap() {
		replicas.remove(myId);
		for (Replica info : replicas) {
			updateServersMap(eventloop, info.getAddress());
		}
	}

	private void updateServersMap(final Eventloop eventloop, final InetSocketAddress address) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + mapUpdateTimeout, new Runnable() {
			@Override
			public void run() {
				client.alive(address, new ResultCallback<Set<Replica>>() {
					@Override
					public void onResult(Set<Replica> result) {
						onShowAliveResponse(eventloop.currentTimeMillis(), result);
					}

					@Override
					public void onException(Exception ignored) {
					}
				});
				updateServersMap(eventloop, address);
			}
		});
	}

	private final class FileInfo {
		Set<Replica> replicas = new HashSet<>();
		FileState state;
		int pendingOperationsCounter;

		FileInfo() {
		}

		FileInfo(Replica myId) {
			state = READY;
			replicas.add(myId);
		}
	}

	enum FileState {
		UPLOADING, TOMBSTONE, READY
	}
}