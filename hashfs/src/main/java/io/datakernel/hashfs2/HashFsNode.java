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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs2.protocol.ClientProtocol;
import io.datakernel.hashfs2.protocol.ServerProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

class HashFsNode implements Commands, Server {
	private static final Logger logger = LoggerFactory.getLogger(HashFsNode.class);
	private final NioEventloop eventloop;

	private final ClientProtocol clientProtocol;
	private final FileSystem fileSystem;
	private ServerProtocol transport;
	private Logic logic;
	private State state;

	private final long updateTimeout;
	private final long mapUpdateTimeout;

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public HashFsNode(NioEventloop eventloop, FileSystem fileSystem, ClientProtocol clientProtocol,
	                  long updateTimeout, long mapUpdateTimeout) {
		this.eventloop = eventloop;
		this.fileSystem = fileSystem;
		this.clientProtocol = clientProtocol;
		this.updateTimeout = updateTimeout;
		this.mapUpdateTimeout = mapUpdateTimeout;
	}

	public void wire(Logic logic, ServerProtocol transport) {
		logger.info("Wired logic and transport");
		this.logic = logic;
		this.transport = transport;
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		state = State.RUNNING;
		CompletionCallback waiter = AsyncCallbacks.waitAll(3, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Started HashFsServer");
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't start HashFsServer", e);
				state = null;
				callback.onException(e);
			}
		});
		fileSystem.start(waiter);
		logic.start(waiter);
		transport.start(waiter);
	}

	@Override
	public void stop(final CompletionCallback callback) {
		state = State.SHUTDOWN;
		CompletionCallback waiter = AsyncCallbacks.waitAll(2, new CompletionCallback() {
			@Override
			public void onComplete() {
				transport.stop(callback);
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
		fileSystem.stop(waiter);
		logic.stop(waiter);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void upload(final String filePath, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		logger.info("Server received request for upload {}", filePath);

		if (state != State.RUNNING) {
			logger.trace("Refused upload {}. Server is down.", filePath);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (logic.canUpload(filePath)) {
			logic.onUploadStart(filePath);
			fileSystem.saveToTemporary(filePath, producer, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Uploaded to temporary {}", filePath);
					logic.onUploadComplete(filePath);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Failed upload to temporary {}", filePath);
					logic.onUploadFailed(filePath);
					callback.onException(e);
				}
			});
		} else {
			logger.warn("Refused upload {}", filePath);
			callback.onException(new Exception("Can't upload file"));
		}
	}

	@Override
	public void commit(final String filePath, final boolean success, final CompletionCallback callback) {
		logger.info("Server received request for file commit: {}, {}", filePath, success);

		boolean canApprove = logic.canApprove(filePath);

		if (state != State.RUNNING && !canApprove) {
			logger.trace("Refused commit {}. Server is down.", filePath);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (canApprove) {
			if (success) {
				fileSystem.commitTemporary(filePath, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("File saved {}", filePath);
						callback.onComplete();
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't save file {}", filePath, e);
						callback.onException(e);
					}
				});
				logic.onApprove(filePath);
			} else {
				fileSystem.deleteTemporary(filePath, callback);
				logic.onApproveCancel(filePath);
			}
		} else {
			logger.warn("Refused commit {}", filePath);
			callback.onException(new Exception("Can't approve file for commit"));
		}
	}

	@Override
	public void download(final String filePath, StreamConsumer<ByteBuf> consumer, ResultCallback<CompletionCallback> callback) {
		logger.info("Received request for file download {}", filePath);

		if (state != State.RUNNING) {
			logger.trace("Refused download {}. Server is down.", filePath);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (logic.canDownload(filePath)) {
			logic.onDownloadStart(filePath);
			StreamProducer<ByteBuf> producer = fileSystem.get(filePath);
			producer.streamTo(consumer);
			callback.onResult(new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Send successfully {}", filePath);
					logic.onDownloadComplete(filePath);
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't send the file {}", filePath);
					logic.onDownloadFailed(filePath);
				}
			});
		} else {
			logger.warn("Refused download {}", filePath);
			callback.onException(new Exception("Can't download"));
		}
	}

	@Override
	public void delete(final String filePath, final CompletionCallback callback) {
		logger.info("Received request for file deletion {}", filePath);

		if (state != State.RUNNING) {
			logger.trace("Refused delete {}. Server is down.", filePath);
			callback.onException(new Exception("Server is down"));
			return;
		}

		if (logic.canDelete(filePath)) {
			logic.onDeletionStart(filePath);
			fileSystem.delete(filePath, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Successfully deleted {}", filePath);
					logic.onDeleteComplete(filePath);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't delete file {}", filePath);
					logic.onDeleteFailed(filePath);
					callback.onException(e);
				}
			});
		} else {
			logger.warn("Refused fileDeletion {}", filePath);
			callback.onException(new Exception("Can't delete file"));
		}
	}

	@Override
	public void listFiles(ResultCallback<Set<String>> callback) {
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
		logger.info("Received request to show alive servers");

		if (state != State.RUNNING) {
			logger.trace("Refused listing alive servers. Server is down.");
			callback.onException(new Exception("Server is down"));
			return;
		}

		logic.onShowAliveRequest(eventloop.currentTimeMillis(), callback);
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

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void replicate(final String filePath, final ServerInfo server) {
		logger.info("Received command to replicate file {} to server {}", filePath, server);
		StreamProducer<ByteBuf> producer = fileSystem.get(filePath);
		logic.onReplicationStart(filePath);
		clientProtocol.upload(server, filePath, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Successfully replicated file {} to server {}", filePath, server);
				logic.onReplicationComplete(filePath, server);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to replicate file {} to server {}", filePath, server, e);
				logic.onReplicationFailed(filePath, server);
			}
		});
	}

	@Override
	public void delete(final String filePath) {
		logger.info("Received command to delete file {}", filePath);
		fileSystem.delete(filePath, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("File {} deleted by server", filePath);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't delete file {} by server", filePath, e);
			}
		});
	}

	@Override
	public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
		logger.info("Received command to offer {} files (forUpload: {}, forDeletion: {})", server, forUpload.size(), forDeletion.size());
		clientProtocol.offer(server, forUpload, forDeletion, result);
	}

	@Override
	public void updateServerMap(final Set<ServerInfo> bootstrap) {
		logger.info("Updating alive servers map");

		final Set<ServerInfo> possiblyDown = new HashSet<>();
		final Set<ServerInfo> possiblyUp = new HashSet<>();

		final int[] counter = {bootstrap.size()};

		for (final ServerInfo server : bootstrap) {
			clientProtocol.alive(server, new ResultCallback<Set<ServerInfo>>() {
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
	public void scheduleTemporaryFileDeletion(final String filePath, long approveWaitTime) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + approveWaitTime, new Runnable() {
			@Override
			public void run() {
				if (logic.canApprove(filePath)) {
					logic.onApproveCancel(filePath);
					logger.info("Deleting uploaded but not commited file {}", filePath);
					fileSystem.deleteTemporary(filePath, ignoreCompletionCallback());
				}
			}
		});
	}

	@Override
	public void scan(ResultCallback<Set<String>> callback) {
		logger.info("Scanning local");
		fileSystem.list(callback);
	}

	@Override
	public void postUpdate() {
		final long timestamp = eventloop.currentTimeMillis() + updateTimeout;
		eventloop.scheduleBackground(timestamp, new Runnable() {
			@Override
			public void run() {
				logger.info("Updating HashFs system state");
				logic.update(timestamp);
			}
		});
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private enum State {
		RUNNING, SHUTDOWN
	}
}
