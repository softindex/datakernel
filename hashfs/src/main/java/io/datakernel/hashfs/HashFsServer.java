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
import io.datakernel.async.ResultCallback;
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.protocol.HashFsClientProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class HashFsServer implements ServerStatusChangeListeners {

	private static final Logger logger = LoggerFactory.getLogger(HashFsServer.class);

	private final NioEventloop eventloop;
	private final ServerInfo myId;
	private final ExecutorService executor;
	private final DirectoryMetadata metadata;
	private final List<ServerStatusNotifier> statusNotifiers = new ArrayList<>();
	private final HashFsClientProtocol clientProtocol;
	private final List<ServerStatusChangeListeners> serverStatusChangeListeners = new ArrayList<>();
	private final Map<ServerInfo, ServerStatus> idServerInfo = new HashMap<>();
	private final Configuration configuration;
	private final UploadQueue uploadQueue;

	private boolean isAlive = true;

	public HashFsServer(NioEventloop eventloop, ServerInfo myId, List<ServerInfo> allServers, Path storagePath, HashFsClientProtocol clientProtocol,
	                    List<ServerStatusNotifier> serverStatusNotifier, ExecutorService executor,
	                    Configuration configuration) {
		this.eventloop = eventloop;
		this.metadata = DirectoryMetadata.createDirectory(storagePath, myId);
		this.clientProtocol = clientProtocol;

		this.myId = myId;
		this.configuration = configuration;
		this.executor = executor;
		this.uploadQueue = new UploadQueue(myId, metadata);
		this.serverStatusChangeListeners.add(this);
		this.statusNotifiers.addAll(serverStatusNotifier);

		initIdServerMap(allServers);

		for (final ServerStatus serverStatus : idServerInfo.values()) {
			if (serverStatus.serverInfo.serverId == myId.serverId) continue;

			eventloop.post(new Runnable() {
				@Override
				public void run() {
					checkAliveServers(serverStatus);
					pullFileLists(serverStatus);
				}
			});
		}

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				distribute();
			}
		});
	}

	public HashFsServer(NioEventloop eventloop, ServerInfo myId, List<ServerInfo> allServers, Path storagePath, HashFsClientProtocol clientProtocol,
	                    ExecutorService executor, Configuration configuration) {
		this(eventloop, myId, allServers, storagePath, clientProtocol, new ArrayList<ServerStatusNotifier>(), executor, configuration);
	}

	public HashFsServer(NioEventloop eventloop, ServerInfo myId, List<ServerInfo> allServers, Path storagePath, HashFsClientProtocol clientProtocol,
	                    ExecutorService executor) {
		this(eventloop, myId, allServers, storagePath, clientProtocol, new ArrayList<ServerStatusNotifier>(), executor, new Configuration());
	}

	private void onUploadFinished(final String filename) {
		logger.info("FileServer {} uploaded file {}", myId.serverId, filename);
		if (metadata.uploadFinish(filename)) {
			for (ServerStatusNotifier notifier : statusNotifiers) {
				notifier.onFileUploaded(myId, filename);
			}
		}
	}

	public void fileList(final ResultCallback<Set<String>> callback) {
		Set<String> files = new HashSet<>();
		files.addAll(metadata.fileList());
		callback.onResult(files);
	}

	public void getFilesWithReplicas(List<String> interestingFiles, ResultCallback<Map<String, Set<Integer>>> resultCallback) {
		resultCallback.onResult(metadata.getFilesWithReplicas(interestingFiles));
	}

	private void onUploadFail(String filename) {
		metadata.deleteUploadFailedFile(filename);
	}

	public void onDownload(String filename, final ResultCallback<StreamProducer<ByteBuf>> resultCallback) {
		final String destinationFilename = metadata.normalisePath(filename);
		boolean canGetDestinationPath = true;
		Path filePath = null;
		try {
			filePath = metadata.getFilePath(destinationFilename);
		} catch (IOException e) {
			logger.error("Can't get file path", e);
			canGetDestinationPath = false;
		}

		if (!metadata.fileExists(destinationFilename) || !canGetDestinationPath) {
			resultCallback.onException(new Exception("No such file exception"));
			return;
		}

		logger.info("FileServer {} receive command download file {}", myId.serverId, destinationFilename);
		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, filePath);
		resultCallback.onResult(producer);
	}

	public boolean canUpload(String filename) {
		logger.info("Try upload {} to {}", filename, myId.serverId);
		final String destinationFilename = metadata.normalisePath(filename);
		if (!metadata.canUpload(destinationFilename)) {
			logger.info("Can't upload file {} to {}: file already exists", destinationFilename, myId.serverId);
			return false;
		}
		return true;
	}

	public Set<String> onAbsentFiles(Map<String, FileMetadata.FileBaseInfo> interestedFiles) {
		Set<String> absentFiles = new HashSet<>();
		for (Map.Entry<String, FileMetadata.FileBaseInfo> entryRemote : interestedFiles.entrySet()) {
			String filename = entryRemote.getKey();
			FileMetadata.FileBaseInfo remoteFileInfo = entryRemote.getValue();
			if (remoteFileInfo.fileStatus == FileMetadata.FileStatus.READY) {
				if (!metadata.isFileDeletedByUser(filename) && !metadata.fileExists(filename)) {
					absentFiles.add(filename);
				}
			} else if (remoteFileInfo.fileStatus == FileMetadata.FileStatus.TOMBSTONE) {
				deleteFileByUser(filename);
			}
		}
		return absentFiles;
	}

	public void onUpload(final String filename, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		final String destinationFilename = metadata.normalisePath(filename);
		Path destination;
		try {
			destination = metadata.getFilePath(destinationFilename, true);
		} catch (IOException e) {
			logger.error("Can't get destination file path for {}", destinationFilename, e);
			callback.onException(e);
			onUploadFail(filename);
			return;
		}

		if (!metadata.canUpload(destinationFilename)) {
			callback.onException(new Exception("File already exists."));
			return;
		}

		metadata.startUpload(filename);
		StreamConsumer<ByteBuf> diskWrite = StreamFileWriter.createFile(eventloop, executor, destination, true);
		producer.streamTo(diskWrite);

		diskWrite.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				onUploadFinished(destinationFilename);
				logger.info("Server {} send file {} uploaded", myId.serverId, destinationFilename);
				callback.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				onUploadFail(destinationFilename);
				callback.onException(new Exception("Upload fail"));
			}
		});
	}

	@Override
	public void onServerAppeared(ServerInfo serverInfo) {
		logger.debug("FileServer {} find appear of  server {}", myId.serverId, serverInfo.serverId);
	}

	@Override
	public void onServerFails(ServerInfo serverInfo) {
		logger.debug("FileServer {} find fail of server {}", myId.serverId, serverInfo.serverId);

		if (serverInfo.serverId != myId.serverId) {
			metadata.removeReplica(serverInfo);
		} else {
			metadata.clearAllReplicas();
		}
	}

	private void initIdServerMap(Iterable<ServerInfo> servers) {
		for (ServerInfo info : servers) {
			idServerInfo.put(info, new ServerStatus(info, configuration, eventloop.currentTimeMillis(), info.serverId == myId.serverId));
		}
	}

	public void getAliveServers(ResultCallback<List<ServerInfo>> resultCallback) {
		resultCallback.onResult(getAliveServers());
	}

	private List<ServerInfo> getAliveServers() {
		List<ServerInfo> aliveServers = new ArrayList<>();
		for (ServerStatus serverStatus : idServerInfo.values()) {
			if (serverStatus.isAlive(eventloop.currentTimeMillis())) {
				aliveServers.add(serverStatus.serverInfo);
			}
		}
		return aliveServers;
	}

	private void checkAliveServers(final ServerStatus serverStatus) {
		if (!isAlive) return;

		LastServerStatus currentStatus = serverStatus.isAlive(eventloop.currentTimeMillis()) ? LastServerStatus.ALIVE : LastServerStatus.DEAD;
		if (serverStatus.lastServerStatus != currentStatus) {
			if (currentStatus == LastServerStatus.ALIVE) {
				for (ServerStatusChangeListeners statusChange : serverStatusChangeListeners) {
					statusChange.onServerAppeared(serverStatus.serverInfo);
				}
			} else {
				for (ServerStatusChangeListeners statusChange : serverStatusChangeListeners) {
					statusChange.onServerFails(serverStatus.serverInfo);
				}
			}
			serverStatus.setLastServerStatus(currentStatus);
		}

		eventloop.scheduleBackground(eventloop.currentTimeMillis() + configuration.getTickPeriod(), new Runnable() {
			@Override
			public void run() {
				checkAliveServers(serverStatus);
			}
		});
	}

	public void deleteFileByUser(String filename, CompletionCallback callback) {
		logger.info("Server receive delete file {} by user.", filename);
		metadata.deleteFileByUser(filename, callback);
		for (ServerStatusNotifier serverStatusNotifier : statusNotifiers) {
			serverStatusNotifier.onFileDeletedByUser(myId, filename);
		}
	}

	private void deleteFileByUser(String filename) {
		deleteFileByUser(filename, new SimpleCompletionCallback() {
			@Override
			protected void onCompleteOrException() {
			}
		});
	}

	private void pullFileLists(final ServerStatus serverStatus) {
		if (!isAlive) return;

		final DirectoryMetadata.ReshardFiles myFilesBaseInfo = metadata.getReshardInfo(serverStatus.serverInfo, getAliveServers());
		// delete unnecessary files
		for (String filename : myFilesBaseInfo.filesForDelete) {
			logger.info("Server {} delete unnecessary file {}", myId.serverId, filename);
			metadata.deleteFileByServer(filename);
			for (ServerStatusNotifier serverStatusNotifier : statusNotifiers) {
				serverStatusNotifier.onFileDeletedByServer(myId, filename);
			}
		}

		clientProtocol.listAbsent(serverStatus.serverInfo, myFilesBaseInfo.filesForDistribute, new ResultCallback<Set<String>>() {
			@Override
			public void onResult(Set<String> remoteFiles) {
				// Update remote server alive time
				idServerInfo.get(serverStatus.serverInfo).updateTime(eventloop.currentTimeMillis());

				// Delete replica
				metadata.removeReplica(serverStatus.serverInfo);

				// Update replica files
				for (Map.Entry<String, FileMetadata.FileBaseInfo> currentFileBaseInfos : myFilesBaseInfo.filesForDistribute.entrySet()) {
					// If answer not contains file then remote server contains requested file
					String filename = currentFileBaseInfos.getKey();
					if (!remoteFiles.contains(filename)) {
						metadata.updateReplica(serverStatus.serverInfo, filename);
					} else {
						uploadQueue.addTask(new UploadTask(filename, serverStatus.serverInfo));
					}
				}
			}

			@Override
			public void onException(Exception exception) {
				logger.error("On remote files list exception", exception);
				// Do nothing : can't connect server
			}
		});

		eventloop.scheduleBackground(eventloop.currentTimeMillis() + configuration.getTickPeriod(), new Runnable() {
			@Override
			public void run() {
				pullFileLists(serverStatus);
			}
		});
	}

	private void distribute() {
		boolean hasNextTask = true;

		while (hasNextTask) {
			if (!isAlive) return;

			final UploadTask uploadTask = uploadQueue.getNextTask();
			if (uploadTask != null) {
				clientProtocol.upload(uploadTask.serverInfo, uploadTask.filename, new ResultCallback<StreamConsumer<ByteBuf>>() {
					@Override
					public void onResult(StreamConsumer<ByteBuf> consumer) {
						try {
							Path filePath = metadata.getFilePath(uploadTask.filename);
							final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, filePath);
							producer.streamTo(consumer);
							consumer.addCompletionCallback(new SimpleCompletionCallback() {
								@Override
								protected void onCompleteOrException() {
									uploadQueue.finishTask(uploadTask);
								}
							});
						} catch (IOException e) {
							logger.error("Can't get file path for {} on {}", uploadTask.filename, myId.serverId);
						}
					}

					@Override
					public void onException(Exception exception) {
						// try upload file next time
						uploadQueue.finishTask(uploadTask);
					}
				});

			} else {
				hasNextTask = false;
			}
		}

		eventloop.scheduleBackground(eventloop.currentTimeMillis() + configuration.getTickPeriod(), new Runnable() {
			@Override
			public void run() {
				distribute();
			}
		});

	}

	public void shutdown() {
		isAlive = false;
	}

	@Override
	public String toString() {
		StringBuilder status = new StringBuilder();

		status.append("servers\t");
		for (ServerStatus info : idServerInfo.values()) {
			status.append(info.serverInfo.serverId);
			status.append("=");
			status.append(info.isAlive(eventloop.currentTimeMillis()));
			status.append("\t");
		}
		return status.toString();
	}

	private enum LastServerStatus {ALIVE, DEAD}

	private static class UploadQueue {
		private final Queue<UploadTask> uploadTasks = new LinkedList<>();
		private final int MAX_TASKS = 3;
		private final ServerInfo myId;
		private final DirectoryMetadata metadata;
		private final Set<UploadTask> inProgress = new HashSet<>();

		private UploadQueue(ServerInfo myId, DirectoryMetadata metadata) {
			this.myId = myId;
			this.metadata = metadata;
		}

		public UploadTask getNextTask() {
			if (inProgress.size() == MAX_TASKS) return null;
			while (!uploadTasks.isEmpty()) {
				UploadTask nextTask = uploadTasks.poll();
				// file in progress
				if (inProgress.contains(nextTask)) continue;

				// file already replicated or not exists
				if (!metadata.fileExists(nextTask.filename) || metadata.getReplicas(nextTask.filename).contains(nextTask.serverInfo)) {
					continue;
				}
				inProgress.add(nextTask);
				logger.info("In upload progress tasks {} on server {}", inProgress.size(), myId.serverId);
				return nextTask;
			}
			return null;
		}

		public void addTask(UploadTask nextTask) {
			uploadTasks.add(nextTask);
		}

		public void finishTask(UploadTask uploadTask) {
			inProgress.remove(uploadTask);
		}

	}

	// TODO optimization for upload
	private static class UploadTask {

		public final String filename;
		public final ServerInfo serverInfo;

		private UploadTask(String filename, ServerInfo serverInfo) {
			this.filename = filename;
			this.serverInfo = serverInfo;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			UploadTask that = (UploadTask) o;

			if (filename != null ? !filename.equals(that.filename) : that.filename != null) return false;
			return !(serverInfo != null ? !serverInfo.equals(that.serverInfo) : that.serverInfo != null);

		}

		@Override
		public int hashCode() {
			int result = filename != null ? filename.hashCode() : 0;
			result = 31 * result + (serverInfo != null ? serverInfo.hashCode() : 0);
			return result;
		}
	}

	private static class ServerStatus {
		public final ServerInfo serverInfo;
		private final boolean alwaysAlive; // always alive if it's our id
		private final Configuration configuration;
		public LastServerStatus lastServerStatus;
		// At start moment we thing everyone is dead.
		private long lastHeartBeatReceive = 0;

		private ServerStatus(ServerInfo serverInfo, Configuration configuration, long timeStamp, boolean alwaysAlive) {
			this.serverInfo = serverInfo;
			this.configuration = configuration;
			this.lastHeartBeatReceive = timeStamp;
			this.lastServerStatus = LastServerStatus.DEAD;
			this.alwaysAlive = alwaysAlive;
		}

		public void updateTime(long timestamp) {
			this.lastHeartBeatReceive = timestamp;
		}

		public boolean isAlive(long currentTime) {
			return lastHeartBeatReceive + configuration.getServerDieTime() > currentTime || alwaysAlive;
		}

		public void setLastServerStatus(LastServerStatus lastServerStatus) {
			this.lastServerStatus = lastServerStatus;
		}
	}
}
