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
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public class HashFsNode implements Commands, Server {
	private static final Logger logger = LoggerFactory.getLogger(HashFsNode.class);
	private final NioEventloop eventloop;

	private final ClientProtocol clientProtocol;
	private final FileSystem fileSystem;
	private ServerProtocol transport;
	private Logic logic;

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
		logger.info(" Wired logic and transport");
		this.logic = logic;
		this.transport = transport;
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		CompletionCallback waiter = AsyncCallbacks.waitAll(3, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Started HashFsServer");
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't start HashFsServer", e);
				callback.onException(e);
			}
		});
		fileSystem.start(waiter);
		logic.start(waiter);
		transport.start(waiter);
	}

	@Override
	public void stop(final CompletionCallback callback) {
		CompletionCallback waiter = AsyncCallbacks.waitAll(2, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Stopped HashFsServer");
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				logger.error("HashFsServer stopped with exception", e);
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
		logger.info("Server received request for file approve {}", filePath);
		if (logic.canApprove(filePath)) {
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
			} else {
				fileSystem.deleteTemporary(filePath, callback);
			}
		} else {
			logger.warn("Refused commit {}", filePath);
			callback.onException(new Exception("Can't approve file for commit"));
		}
	}

	@Override
	public void download(final String filePath, StreamForwarder<ByteBuf> consumer, ResultCallback<CompletionCallback> callback) {
		logger.info("Received request for file download {}", filePath);
		if (logic.canDownload(filePath)) {
			logic.onDownloadStart(filePath);
			fileSystem.get(filePath).streamTo(consumer);
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
			callback.onResult(ignoreCompletionCallback());
			logic.onDownloadFailed(filePath);
			StreamProducers.<ByteBuf>closingWithError(eventloop, new Exception("Can't ")).streamTo(consumer);
		}
	}

	@Override
	public void delete(final String filePath, final CompletionCallback callback) {
		logger.info("Received request for file deletion {}", filePath);
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
		fileSystem.list(callback);
	}

	@Override
	public void showAlive(ResultCallback<Set<ServerInfo>> callback) {
		logger.info("Received request to show alive servers");
		logic.onShowAliveRequest(callback);
	}

	@Override
	public void checkOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		logger.info("Received offer (forUpload: {}, forDeletion: {})", forUpload.size(), forDeletion.size());
		logic.onOfferRequest(forUpload, forDeletion, callback);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void replicate(final String filePath, final ServerInfo server) {
		logger.info("Received command to replicate file {} to server {}", filePath, server);
		StreamProducer<ByteBuf> producer = fileSystem.get(filePath);
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
				logger.info("File {} deleted", filePath);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't delete file {}", filePath, e);
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
		for (final ServerInfo server : bootstrap) {
			clientProtocol.alive(server, new ResultCallback<Set<ServerInfo>>() {
				@Override
				public void onResult(Set<ServerInfo> result) {
					logger.info("Received {} alive servers from {}", result.size(), server);
					for (ServerInfo s : result) {
						s.updateState(ServerInfo.ServerStatus.RUNNING, eventloop.currentTimeMillis());
					}
					logic.onShowAliveResponse(result, eventloop.currentTimeMillis());
				}

				@Override
				public void onException(Exception ignored) {
					logger.warn("Server {} doesn't answer", server);
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
		eventloop.schedule(eventloop.currentTimeMillis() + updateTimeout, new Runnable() {
			@Override
			public void run() {
				logger.info("Updating...");
				logic.update();
			}
		});
	}
}
