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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public class HashFS implements Commands, Server {
	private static final Logger logger = LoggerFactory.getLogger(HashFS.class);

	private static final long APPROVE_WAIT_TIME = 10 * 1000;
	private static final long SERVER_UPDATE_TIME = 100 * 1000;
	private static final long TIMEOUT_TO_UPDATE = 50 * 1000;

	private final NioEventloop eventloop;
	private final FileSystem fileSystem;
	private final Client client;
	private Logic logic;

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public HashFS(NioEventloop eventloop, FileSystem fileSystem, Client client) {
		this.eventloop = eventloop;
		//this.logic = logic;
		this.fileSystem = fileSystem;
		this.client = client;
	}

	public void wireLogic(Logic logic) {
		this.logic = logic;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void upload(final String filePath, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		if (logic.canUpload(filePath)) {
			logic.onUploadStart(filePath);
			fileSystem.saveToTemporary(filePath, producer, new CompletionCallback() {
				@Override
				public void onComplete() {
					logic.onUploadComplete(filePath);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logic.onUploadFailed(filePath);
				}
			});
		} else {
			callback.onException(new Exception("Can't upload file"));
		}
	}

	@Override
	public void commit(final String filePath, final boolean success, final CompletionCallback callback) {
		if (logic.canApprove(filePath)) {
			CompletionCallback transit = new CompletionCallback() {
				@Override
				public void onComplete() {
					logic.onApprove(filePath);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logic.onApproveCancel(filePath);
					callback.onException(e);
				}
			};
			if (success) {
				fileSystem.commitTemporary(filePath, transit);
			} else {
				fileSystem.deleteTemporary(filePath, transit);
			}
		} else {
			callback.onException(new Exception("Can't approve file for commit"));
		}
	}

	@Override
	public void download(final String filePath, StreamForwarder<ByteBuf> consumer, ResultCallback<CompletionCallback> crutch) {
		if (logic.canDownload(filePath)) {
			logic.onDownloadStart(filePath);
			fileSystem.get(filePath, consumer);
			crutch.onResult(new CompletionCallback() {
				@Override
				public void onComplete() {
					logic.onDownloadComplete(filePath);
				}

				@Override
				public void onException(Exception e) {
					logic.onDownloadFailed(filePath);
				}
			});
		} else {
			crutch.onResult(new CompletionCallback() {
				@Override
				public void onComplete() {
					logic.onDownloadComplete(filePath);
				}

				@Override
				public void onException(Exception e) {
					logic.onDownloadFailed(filePath);
				}
			});
			StreamProducers.<ByteBuf>closingWithError(eventloop, new Exception("Can't ")).streamTo(consumer);
		}
	}

	@Override
	public void delete(final String filePath, final CompletionCallback callback) {
		if (logic.canDelete(filePath)) {
			logic.onDeletionStart(filePath);
			fileSystem.deleteFile(filePath, new CompletionCallback() {
				@Override
				public void onComplete() {
					logic.onDeleteComplete(filePath);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logic.onDeleteFailed(filePath);
					callback.onException(e);
				}
			});
		} else {
			callback.onException(new Exception("Can't delete file"));
		}
	}

	@Override
	public void listFiles(ResultCallback<Set<String>> callback) {
		fileSystem.listFiles(callback);
	}

	@Override
	public void showAlive(ResultCallback<Set<ServerInfo>> callback) {
		logic.onShowAliveRequest(callback);
	}

	@Override
	public void checkOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		logic.onOfferRequest(forUpload, forDeletion, callback);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void replicate(final String filePath, final ServerInfo server) {
		StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		fileSystem.get(filePath, forwarder);
		client.upload(server, filePath, forwarder, new CompletionCallback() {
			@Override
			public void onComplete() {
				logic.onReplicationComplete(filePath, server);
			}

			@Override
			public void onException(Exception e) {
				logic.onReplicationFailed(filePath, server);
			}
		});
	}

	@Override
	public void delete(String filePath) {
		// TODO
	}

	@Override
	public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
		client.offer(server, forUpload, forDeletion, result);
	}

	@Override
	public void updateServerMap(final Set<ServerInfo> bootstrap) {
		for (ServerInfo server : bootstrap) {
			client.alive(server, new ResultCallback<Set<ServerInfo>>() {
				@Override
				public void onResult(Set<ServerInfo> result) {
					logic.onShowAliveResponse(result, eventloop.currentTimeMillis());
				}

				@Override
				public void onException(Exception ignored) {

				}
			});
		}
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				updateServerMap(bootstrap);
			}
		});
	}

	@Override
	public void scheduleTemporaryFileDeletion(final String filePath) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + APPROVE_WAIT_TIME, new Runnable() {
			@Override
			public void run() {
				commit(filePath, false, ignoreCompletionCallback());
			}
		});
	}

	@Override
	public void scan(ResultCallback<Set<String>> callback) {
		fileSystem.listFiles(callback);
	}

	@Override
	public void postUpdate() {
		eventloop.schedule(eventloop.currentTimeMillis() + TIMEOUT_TO_UPDATE, new Runnable() {
			@Override
			public void run() {
				logic.update();
			}
		});
	}
}
