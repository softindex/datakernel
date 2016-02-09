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

import io.datakernel.FileSystem;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.file.AsyncFile;
import io.datakernel.net.SocketSettings;
import io.datakernel.protocol.FsServer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
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

import static io.datakernel.util.Preconditions.*;

public class HashFsServer extends FsServer implements Commands, EventloopService {
	public final static class Builder {
		private final Eventloop eventloop;

		private final HashFsClientProtocol.Builder clientBuilder;
		private final HashFsServerProtocol.Builder serverBuilder;
		private final LogicImpl.Builder logicBuilder;
		private final List<InetSocketAddress> addresses = new ArrayList<>();

		private long systemUpdateTimeout = DEFAULT_SYSTEM_UPDATE_TIMEOUT;
		private long mapUpdateTimeout = DEFAULT_MAP_UPDATE_TIMEOUT;

		private int bufferSize = DEFAULT_READER_BUFFER_SIZE;

		private HashFsClientProtocol clientProtocol;
		private HashFsServerProtocol serverProtocol;
		private Logic logic;

		private FileSystem fileSystem;
		private ExecutorService executor;
		private Path storage;

		private Builder(Eventloop eventloop, ExecutorService executor,
		                Path storage, ServerInfo myId, Set<ServerInfo> bootstrap) {
			this.eventloop = eventloop;
			this.clientBuilder = HashFsClientProtocol.build(eventloop);
			this.serverBuilder = HashFsServerProtocol.build(eventloop);
			this.executor = executor;
			this.storage = storage;
			this.logicBuilder = LogicImpl.build(myId, bootstrap);
			addresses.add(myId.getAddress());
		}

		public void logic(Logic logic) {
			this.logic = logic;
		}

		public void bufferSize(int bufferSize) {
			this.bufferSize = bufferSize;
		}

		public Builder listenAddress(InetSocketAddress address) {
			this.addresses.add(address);
			return this;
		}

		public Builder listenAddresses(List<InetSocketAddress> addresses) {
			this.addresses.addAll(addresses);
			return this;
		}

		public Builder listenPort(int port) {
			this.addresses.add(new InetSocketAddress(port));
			return this;
		}

		public Builder systemUpdateTimeout(long systemUpdateTimeout) {
			this.systemUpdateTimeout = systemUpdateTimeout;
			return this;
		}

		public Builder mapUpdateTimeout(long mapUpdateTimeout) {
			this.mapUpdateTimeout = mapUpdateTimeout;
			return this;
		}

		public Builder clientProtocol(HashFsClientProtocol clientProtocol) {
			this.clientProtocol = clientProtocol;
			return this;
		}

		public Builder serverProtocol(HashFsServerProtocol serverProtocol) {
			this.serverProtocol = serverProtocol;
			return this;
		}

		public Builder fileSystem(FileSystem fileSystem) {
			this.fileSystem = fileSystem;
			return this;
		}

		public Builder minChunkSize(int minChunkSize) {
			clientBuilder.setMinChunkSize(minChunkSize);
			return this;
		}

		public Builder socketSettings(SocketSettings socketSettings) {
			clientBuilder.setSocketSettings(socketSettings);
			return this;
		}

		public Builder deserializerBufferSize(int deserializerBufferSize) {
			clientBuilder.setDeserializerBufferSize(deserializerBufferSize);
			return this;
		}

		public Builder maxChunkSize(int maxChunkSize) {
			clientBuilder.setMaxChunkSize(maxChunkSize);
			return this;
		}

		public Builder connectTimeout(int connectTimeout) {
			clientBuilder.setConnectTimeout(connectTimeout);
			return this;
		}

		public Builder serializerMaxMessageSize(int serializerMaxMessageSize) {
			clientBuilder.setSerializerMaxMessageSize(serializerMaxMessageSize);
			return this;
		}

		public Builder serializerBufferSize(int serializerBufferSize) {
			clientBuilder.setSerializerBufferSize(serializerBufferSize);
			return this;
		}

		public Builder serializerFlushDelayMillis(int serializerFlushDelayMillis) {
			clientBuilder.setSerializerFlushDelayMillis(serializerFlushDelayMillis);
			return this;
		}

		public Builder serverDeathTimeout(long serverDeathTimeout) {
			logicBuilder.setServerDeathTimeout(serverDeathTimeout);
			return this;
		}

		public Builder maxReplicaQuantity(int maxReplicaQuantity) {
			logicBuilder.setMaxReplicaQuantity(maxReplicaQuantity);
			return this;
		}

		public Builder minSafeReplicaQuantity(int minSafeReplicaQuantity) {
			logicBuilder.setMinSafeReplicaQuantity(minSafeReplicaQuantity);
			return this;
		}

		public Builder hashing(HashingStrategy hashing) {
			logicBuilder.setHashing(hashing);
			return this;
		}

		public HashFsServer build() {
			HashFsClientProtocol cp = clientProtocol == null ? clientBuilder.build() : clientProtocol;
			HashFsServerProtocol sp = serverProtocol == null ? serverBuilder.build() : serverProtocol;

			FileSystem fs = fileSystem != null ? fileSystem : FileSystem.newInstance(eventloop, executor, storage);

			Logic l = logic == null ? logicBuilder.build() : logic;

			sp.setListenAddresses(addresses);

			HashFsServer server = new HashFsServer(eventloop, fs, l, cp, sp, systemUpdateTimeout, mapUpdateTimeout, bufferSize);
			l.wire(server);
			sp.wire(server);
			return server;
		}
	}

	private static final long DEFAULT_SYSTEM_UPDATE_TIMEOUT = 10 * 100;
	private static final long DEFAULT_MAP_UPDATE_TIMEOUT = 10 * 100;
	public static final int DEFAULT_READER_BUFFER_SIZE = 256 * 1024;

	private static final Logger logger = LoggerFactory.getLogger(HashFsServer.class);
	private final Eventloop eventloop;

	private final HashFsClientProtocol clientProtocol;
	private final HashFsServerProtocol serverProtocol;
	private final FileSystem fileSystem;
	private final Logic logic;
	private State state;

	private final int bufferSize;

	private final long systemUpdateTimeout;
	private final long mapUpdateTimeout;

	// creators
	private HashFsServer(Eventloop eventloop, FileSystem fileSystem, Logic logic,
	                     HashFsClientProtocol clientProtocol, HashFsServerProtocol serverProtocol,
	                     long systemUpdateTimeout, long mapUpdateTimeout, int bufferSize) {
		this.eventloop = checkNotNull(eventloop);
		this.fileSystem = checkNotNull(fileSystem);
		this.logic = checkNotNull(logic);
		this.clientProtocol = checkNotNull(clientProtocol);
		this.serverProtocol = checkNotNull(serverProtocol);
		check(systemUpdateTimeout > 0, "System update timeout should be positive");
		this.systemUpdateTimeout = systemUpdateTimeout;
		check(mapUpdateTimeout > 0, "Alive servers map update timeout should be positive");
		this.mapUpdateTimeout = mapUpdateTimeout;
		this.bufferSize = bufferSize;
	}

	public static HashFsServer newInstance(Eventloop eventloop, ExecutorService executor,
	                                       Path storage, ServerInfo myId, Set<ServerInfo> bootstrap) {
		return build(eventloop, executor, storage, myId, bootstrap).build();
	}

	public static Builder build(Eventloop eventloop, ExecutorService executor,
	                            Path storage, ServerInfo myId, Set<ServerInfo> bootstrap) {
		return new Builder(eventloop, executor, storage, myId, bootstrap);
	}

	// eventloop service methods
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		logger.info("Starting HashFs");
		try {
			fileSystem.initDirectories();
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
		logger.info("Stopping HashFs");
		state = State.SHUTDOWN;
		logic.stop(new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				serverProtocol.close();
				callback.onComplete();
			}
		});
	}

	// api
	@Override
	public void upload(final String fileName, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		logger.info("Server received request for upload {}", fileName);
		checkState(state == State.RUNNING, "Server shut down!");
		if (logic.canUpload(fileName)) {
			logic.onUploadStart(fileName);
			fileSystem.save(fileName, new ResultCallback<AsyncFile>() {
				@Override
				public void onResult(AsyncFile result) {
					StreamFileWriter writer = StreamFileWriter.create(eventloop, result);
					producer.streamTo(writer);
					writer.setFlushCallback(new ForwardingCompletionCallback(this) {
						@Override
						public void onComplete() {
							logger.info("Uploaded {}", fileName);
							logic.onUploadComplete(fileName);
							callback.onComplete();
						}
					});
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
	protected void download(final String fileName, final long startPosition, final ResultCallback<StreamProducer<ByteBuf>> callback) {
		logger.info("Received request for file download {}", fileName);

		checkState(state == State.RUNNING, "Server shut down!");

		if (logic.canDownload(fileName)) {
			logic.onDownloadStart(fileName);
			fileSystem.get(fileName, new ResultCallback<AsyncFile>() {
				@Override
				public void onResult(AsyncFile result) {
					StreamFileReader reader = StreamFileReader.readFileFrom(eventloop, result, bufferSize, startPosition);
					callback.onResult(reader);
					reader.setPositionCallback(new ForwardingResultCallback<Long>(this) {
						@Override
						public void onResult(Long result) {
							logger.info("File {} send successfully {} bytes", fileName, result);
							logic.onDownloadComplete(fileName);
						}
					});
				}

				@Override
				public void onException(Exception exception) {
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

		checkState(state == State.RUNNING, "Server shut down!");

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
	public void list(ResultCallback<List<String>> callback) {
		logger.info("Received request to list files");
		checkState(state == State.RUNNING, "Server shut down!");
		fileSystem.list(callback);
	}

	public void showAlive(ResultCallback<Set<ServerInfo>> callback) {
		logger.trace("Received request to show alive servers");
		checkState(state == State.RUNNING, "Server shut down!");
		logic.onShowAliveRequest(eventloop.currentTimeMillis(), callback);
	}

	@Override
	public void fileSize(String fileName, ResultCallback<Long> callback) {
		fileSystem.fileSize(fileName, callback);
	}

	public void checkOffer(List<String> forUpload, List<String> forDeletion, ResultCallback<List<String>> callback) {
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
		logic.onReplicationStart(fileName);
		fileSystem.get(fileName, new ResultCallback<AsyncFile>() {
			@Override
			public void onResult(AsyncFile result) {
				StreamFileReader reader = StreamFileReader.readFileFully(eventloop, result, bufferSize);
				clientProtocol.upload(server.getAddress(), fileName, reader, new ForwardingCompletionCallback(this) {
					@Override
					public void onComplete() {
						logger.info("Successfully replicated file {} to server {}", fileName, server);
						logic.onReplicationComplete(server, fileName);
					}
				});
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
	public void offer(ServerInfo server, List<String> forUpload, List<String> forDeletion,
	                  ResultCallback<List<String>> callback) {
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
	public void scan(ResultCallback<List<String>> callback) {
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
