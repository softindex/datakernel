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
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public class HashFsServer implements Commands, FsServer, NioService {
	public final static class Builder {
		private final NioEventloop eventloop;

		private final GsonClientProtocol.Builder clientBuilder;
		private final GsonServerProtocol.Builder serverBuilder;
		private final FileSystemImpl.Builder fsBuilder;
		private final LogicImpl.Builder logicBuilder;
		private final List<InetSocketAddress> addresses = new ArrayList<>();

		private long systemUpdateTimeout = DEFAULT_SYSTEM_UPDATE_TIMEOUT;
		private long mapUpdateTimeout = DEFAULT_MAP_UPDATE_TIMEOUT;
		private ClientProtocol clientProtocol;
		private ServerProtocol serverProtocol;
		private FileSystem fileSystem;
		private Logic logic;

		private Builder(NioEventloop eventloop, ExecutorService executor,
		                Path storage, ServerInfo myId, Set<ServerInfo> bootstrap) {
			this.eventloop = eventloop;
			this.clientBuilder = GsonClientProtocol.buildInstance(eventloop);
			this.serverBuilder = GsonServerProtocol.buildInstance(eventloop);
			this.fsBuilder = FileSystemImpl.buildInstance(eventloop, executor, storage);
			this.logicBuilder = LogicImpl.buildInstance(myId, bootstrap);
			addresses.add(myId.getAddress());
		}

		public void setLogic(Logic logic) {
			this.logic = logic;
		}

		public Builder specifyListenAddress(InetSocketAddress address) {
			this.addresses.add(address);
			return this;
		}

		public Builder specifyListenAddresses(List<InetSocketAddress> addresses) {
			this.addresses.addAll(addresses);
			return this;
		}

		public Builder specifyListenPort(int port) {
			this.addresses.add(new InetSocketAddress(port));
			return this;
		}

		public Builder setSystemUpdateTimeout(long systemUpdateTimeout) {
			this.systemUpdateTimeout = systemUpdateTimeout;
			return this;
		}

		public Builder setMapUpdateTimeout(long mapUpdateTimeout) {
			this.mapUpdateTimeout = mapUpdateTimeout;
			return this;
		}

		public Builder setClientProtocol(ClientProtocol clientProtocol) {
			this.clientProtocol = clientProtocol;
			return this;
		}

		public Builder setServerProtocol(ServerProtocol serverProtocol) {
			this.serverProtocol = serverProtocol;
			return this;
		}

		public Builder setFileSystem(FileSystem fileSystem) {
			this.fileSystem = fileSystem;
			return this;
		}

		public Builder setMinChunkSize(int minChunkSize) {
			clientBuilder.setMinChunkSize(minChunkSize);
			return this;
		}

		public Builder setSocketSettings(SocketSettings socketSettings) {
			clientBuilder.setSocketSettings(socketSettings);
			return this;
		}

		public Builder setDeserializerBufferSize(int deserializerBufferSize) {
			clientBuilder.setDeserializerBufferSize(deserializerBufferSize);
			return this;
		}

		public Builder setMaxChunkSize(int maxChunkSize) {
			clientBuilder.setMaxChunkSize(maxChunkSize);
			return this;
		}

		public Builder setConnectTimeout(int connectTimeout) {
			clientBuilder.setConnectTimeout(connectTimeout);
			return this;
		}

		public Builder setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			clientBuilder.setSerializerMaxMessageSize(serializerMaxMessageSize);
			return this;
		}

		public Builder setSerializerBufferSize(int serializerBufferSize) {
			clientBuilder.setSerializerBufferSize(serializerBufferSize);
			return this;
		}

		public Builder setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			clientBuilder.setSerializerFlushDelayMillis(serializerFlushDelayMillis);
			return this;
		}

		public Builder setInProgressExtension(String inProgressExtension) {
			fsBuilder.setInProgressExtension(inProgressExtension);
			return this;
		}

		public Builder setTmpStorage(Path tmpStorage) {
			fsBuilder.setTmpStorage(tmpStorage);
			return this;
		}

		public Builder setReaderBufferSize(int readerBufferSize) {
			fsBuilder.setReaderBufferSize(readerBufferSize);
			return this;
		}

		public Builder setTmpDirectoryName(String tmpDirectoryName) {
			fsBuilder.setTmpDirectoryName(tmpDirectoryName);
			return this;
		}

		public Builder setServerDeathTimeout(long serverDeathTimeout) {
			logicBuilder.setServerDeathTimeout(serverDeathTimeout);
			return this;
		}

		public Builder setApproveWaitTime(long approveWaitTime) {
			logicBuilder.setApproveWaitTime(approveWaitTime);
			return this;
		}

		public Builder setMaxReplicaQuantity(int maxReplicaQuantity) {
			logicBuilder.setMaxReplicaQuantity(maxReplicaQuantity);
			return this;
		}

		public Builder setMinSafeReplicaQuantity(int minSafeReplicaQuantity) {
			logicBuilder.setMinSafeReplicaQuantity(minSafeReplicaQuantity);
			return this;
		}

		public Builder setHashing(HashingStrategy hashing) {
			logicBuilder.setHashing(hashing);
			return this;
		}

		public HashFsServer build() {
			ClientProtocol cp = clientProtocol == null ? clientBuilder.build() : clientProtocol;
			ServerProtocol sp = serverProtocol == null ? serverBuilder.build() : serverProtocol;
			FileSystem fs = fileSystem == null ? fsBuilder.build() : fileSystem;
			Logic l = logic == null ? logicBuilder.build() : logic;

			sp.setListenAddresses(addresses);

			HashFsServer server = new HashFsServer(eventloop, fs, l, cp, sp, systemUpdateTimeout, mapUpdateTimeout);
			l.wire(server);
			sp.wire(server);
			return server;
		}
	}

	private static final long DEFAULT_SYSTEM_UPDATE_TIMEOUT = 10 * 1000;
	private static final long DEFAULT_MAP_UPDATE_TIMEOUT = 10 * 1000;

	private static final Logger logger = LoggerFactory.getLogger(HashFsServer.class);
	private final NioEventloop eventloop;

	private final ClientProtocol clientProtocol;
	private final ServerProtocol serverProtocol;
	private final FileSystem fileSystem;
	private final Logic logic;
	private State state;

	private final long systemUpdateTimeout;
	private final long mapUpdateTimeout;

	private HashFsServer(NioEventloop eventloop, FileSystem fileSystem, Logic logic,
	                     ClientProtocol clientProtocol, ServerProtocol serverProtocol,
	                     long systemUpdateTimeout, long mapUpdateTimeout) {
		this.eventloop = eventloop;
		this.fileSystem = fileSystem;
		this.logic = logic;
		this.clientProtocol = clientProtocol;
		this.serverProtocol = serverProtocol;
		this.systemUpdateTimeout = systemUpdateTimeout;
		this.mapUpdateTimeout = mapUpdateTimeout;
	}

	public static HashFsServer createInstance(NioEventloop eventloop, ExecutorService executor,
	                                          Path storage, ServerInfo myId, Set<ServerInfo> bootstrap) {
		return buildInstance(eventloop, executor, storage, myId, bootstrap).build();
	}

	public static Builder buildInstance(NioEventloop eventloop, ExecutorService executor,
	                                    Path storage, ServerInfo myId, Set<ServerInfo> bootstrap) {
		return new Builder(eventloop, executor, storage, myId, bootstrap);
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		try {
			fileSystem.ensureInfrastructure();
			serverProtocol.listen();
			logic.start(new CompletionCallback() {
				@Override
				public void onComplete() {
					state = State.RUNNING;
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					serverProtocol.close();
					callback.onException(e);
				}
			});
		} catch (IOException e) {
			callback.onException(e);
		}
	}

	@Override
	public void stop(final CompletionCallback callback) {
		state = State.SHUTDOWN;
		logic.stop(new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				serverProtocol.close();
				callback.onComplete();
			}
		});
	}

	@Override
	public void upload(final String fileName, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		logger.info("Server received request for upload {}", fileName);

		if (state != State.RUNNING) {
			logger.trace("Refused upload {}. Server is down.", fileName);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (logic.canUpload(fileName)) {
			logic.onUploadStart(fileName);
			fileSystem.saveToTmp(fileName, producer, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Uploaded to temporary {}", fileName);
					logic.onUploadComplete(fileName);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Failed upload to temporary {}", fileName);
					logic.onUploadFailed(fileName);
					callback.onException(e);
				}
			});
		} else {
			logger.warn("Refused upload {}", fileName);
			callback.onException(new Exception("Can't upload file"));
		}
	}

	@Override
	public void commit(final String fileName, final boolean success, final CompletionCallback callback) {
		logger.info("Server received request for file commit: {}, {}", fileName, success);

		boolean canApprove = logic.canApprove(fileName);

		if (state != State.RUNNING && !canApprove) {
			logger.trace("Refused commit {}. Server is down.", fileName);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (canApprove) {
			if (success) {
				fileSystem.commitTmp(fileName, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("File saved {}", fileName);
						callback.onComplete();
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't save file {}", fileName, e);
						callback.onException(e);
					}
				});
				logic.onApprove(fileName);
			} else {
				fileSystem.deleteTmp(fileName, callback);
				logic.onApproveCancel(fileName);
			}
		} else {
			logger.warn("Refused commit: {}, {}", fileName, success);
			callback.onException(new Exception("Can't approve file for commit"));
		}
	}

	@Override
	public void download(final String fileName, StreamConsumer<ByteBuf> consumer, ResultCallback<CompletionCallback> callback) {
		logger.info("Received request for file download {}", fileName);

		if (state != State.RUNNING) {
			logger.trace("Refused download {}. Server is down.", fileName);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (logic.canDownload(fileName)) {
			logic.onDownloadStart(fileName);
			StreamProducer<ByteBuf> producer = fileSystem.get(fileName);
			producer.streamTo(consumer);
			callback.onResult(new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Send successfully {}", fileName);
					logic.onDownloadComplete(fileName);
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't send the file {}", fileName);
					logic.onDownloadFailed(fileName);
				}
			});
		} else {
			logger.warn("Refused download {}", fileName);
			callback.onException(new Exception("Can't download"));
		}
	}

	@Override
	public void delete(final String fileName, final CompletionCallback callback) {
		logger.info("Received request for file deletion {}", fileName);

		if (state != State.RUNNING) {
			logger.trace("Refused delete {}. Server is down.", fileName);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (logic.canDelete(fileName)) {
			logic.onDeletionStart(fileName);
			fileSystem.delete(fileName, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Successfully deleted {}", fileName);
					logic.onDeleteComplete(fileName);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't delete file {}", fileName);
					logic.onDeleteFailed(fileName);
					callback.onException(e);
				}
			});
		} else {
			logger.warn("Refused file deletion {}", fileName);
			callback.onException(new Exception("Can't delete file"));
		}
	}

	@Override
	public void list(ResultCallback<Set<String>> callback) {
		logger.info("Received request to list files");

		if (state != State.RUNNING) {
			logger.trace("Refused listing files. Server is down.");
			callback.onException(new Exception("Server is down"));
			return;
		}

		fileSystem.list(callback);
	}

	@Override
	public void showAlive(ResultCallback<Set<ServerInfo>> callback) {
		logger.trace("Received request to show alive servers");

		if (state != State.RUNNING) {
			logger.trace("Refused listing alive servers. Server is down.");
			callback.onException(new Exception("Server is down"));
			return;
		}

		logic.onShowAliveRequest(eventloop.currentTimeMillis(), callback);
	}

	@Override
	public long fileSize(String filePath) {
		return fileSystem.exists(filePath);
	}

	@Override
	public void checkOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		logger.info("Received offer (forUpload: {}, forDeletion: {})", forUpload.size(), forDeletion.size());

		if (state != State.RUNNING) {
			logger.trace("Refused checking offer. Server is down.");
			callback.onException(new Exception("Server is down"));
			return;
		}

		logic.onOfferRequest(forUpload, forDeletion, callback);
	}

	@Override
	public void replicate(final ServerInfo server, final String fileName) {
		logger.info("Received command to replicate file {} to server {}", fileName, server);
		StreamProducer<ByteBuf> producer = fileSystem.get(fileName);
		logic.onReplicationStart(fileName);
		clientProtocol.upload(server.getAddress(), fileName, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Successfully replicated file {} to server {}", fileName, server);
				logic.onReplicationComplete(server, fileName);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to replicate file {} to server {}", fileName, server, e);
				logic.onReplicationFailed(server, fileName);
			}
		});
	}

	@Override
	public void delete(final String fileName) {
		logger.info("Received command to delete file {}", fileName);
		fileSystem.delete(fileName, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("File {} deleted by server", fileName);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't delete file {} by server", fileName, e);
			}
		});
	}

	@Override
	public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion,
	                  ResultCallback<Set<String>> callback) {
		logger.info("Received command to offer {} files (forUpload: {}, forDeletion: {})", server, forUpload.size(), forDeletion.size());
		clientProtocol.offer(server.getAddress(), forUpload, forDeletion, callback);
	}

	@Override
	public void updateServerMap(final Set<ServerInfo> bootstrap) {
		logger.trace("Updating alive servers map");

		final Set<ServerInfo> possiblyDown = new HashSet<>();
		final Set<ServerInfo> possiblyUp = new HashSet<>();

		final int[] counter = {bootstrap.size()};

		for (final ServerInfo server : bootstrap) {
			clientProtocol.alive(server.getAddress(), new ResultCallback<Set<ServerInfo>>() {
				@Override
				public void onResult(Set<ServerInfo> result) {
					logger.trace("Received {} alive servers from {}", result.size(), server);
					possiblyUp.addAll(result);
					counter[0]--;
					if (counter[0] == 0) {
						for (ServerInfo server : possiblyDown) {
							possiblyUp.remove(server);
						}
						logic.onShowAliveResponse(eventloop.currentTimeMillis(), possiblyUp);
					}
				}

				@Override
				public void onException(Exception ignored) {
					possiblyDown.add(server);
					logger.warn("Server {} doesn't answer", server);
					counter[0]--;
					if (counter[0] == 0) {
						for (ServerInfo server : possiblyDown) {
							possiblyUp.remove(server);
						}
						logic.onShowAliveResponse(eventloop.currentTimeMillis(), possiblyUp);
					}
				}
			});
		}

		eventloop.scheduleBackground(eventloop.currentTimeMillis() + mapUpdateTimeout, new Runnable() {
			@Override
			public void run() {
				updateServerMap(bootstrap);
			}
		});
	}

	@Override
	public void scheduleCommitCancel(final String fileName, long approveWaitTime) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + approveWaitTime, new Runnable() {
			@Override
			public void run() {
				if (logic.canApprove(fileName)) {
					logic.onApproveCancel(fileName);
					logger.info("Deleting uploaded but not commited file {}", fileName);
					fileSystem.deleteTmp(fileName, ignoreCompletionCallback());
				}
			}
		});
	}

	@Override
	public void scan(ResultCallback<Set<String>> callback) {
		logger.trace("Scanning local");
		fileSystem.list(callback);
	}

	@Override
	public void scheduleUpdate() {
		final long timestamp = eventloop.currentTimeMillis() + systemUpdateTimeout;
		eventloop.scheduleBackground(timestamp, new Runnable() {
			@Override
			public void run() {
				logger.trace("Updating HashFs system state");
				logic.update(timestamp);
			}
		});
	}

	private enum State {
		RUNNING, SHUTDOWN
	}
}
