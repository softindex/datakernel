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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.protocol.ClientProtocol;
import io.datakernel.hashfs.protocol.ServerProtocol;
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
			logger.warn("Refused commit {}", fileName);
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
			logger.warn("Refused fileDeletion {}", fileName);
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
	public void replicate(final ServerInfo server, final String filePath) {
		logger.info("Received command to replicate file {} to server {}", filePath, server);
		StreamProducer<ByteBuf> producer = fileSystem.get(filePath);
		logic.onReplicationStart(filePath);
		clientProtocol.upload(server, filePath, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Successfully replicated file {} to server {}", filePath, server);
				logic.onReplicationComplete(server, filePath);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to replicate file {} to server {}", filePath, server, e);
				logic.onReplicationFailed(server, filePath);
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
	public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		logger.info("Received command to offer {} files (forUpload: {}, forDeletion: {})", server, forUpload.size(), forDeletion.size());
		clientProtocol.offer(server, forUpload, forDeletion, callback);
	}

	@Override
	public void updateServerMap(final Set<ServerInfo> bootstrap) {
		logger.trace("Updating alive servers map");

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
		final long timestamp = eventloop.currentTimeMillis() + updateTimeout;
		eventloop.scheduleBackground(timestamp, new Runnable() {
			@Override
			public void run() {
				logger.trace("Updating HashFs system state");
				logic.update(timestamp);
			}
		});
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private enum State {
		RUNNING, SHUTDOWN
	}
}
