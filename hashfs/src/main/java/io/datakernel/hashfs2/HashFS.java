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
import io.datakernel.hashfs2.net.Protocol;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public class HashFS implements Commands, Server {
	private static final long TIME_TO_SLAY = 10 * 1000;
	private static final long SERVER_UPDATE_TIME = 100 * 1000;
	private static final long TIMEOUT_TO_UPDATE = 50 * 1000;
	private final NioEventloop eventloop;
	private Logic logic;
	private final FileSystem fileSystem;
	private final Protocol protocol;

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public HashFS(NioEventloop eventloop, FileSystem fileSystem, Protocol protocol) {
		this.eventloop = eventloop;
		//this.logic = logic;
		this.fileSystem = fileSystem;
		this.protocol = protocol;
	}

	public void wirelogic(Logic logic) {
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
			fileSystem.commitTemporary(filePath, success, new CompletionCallback() {
				@Override
				public void onComplete() {
					logic.onApprove(filePath, success);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					logic.onApprove(filePath, false);
					callback.onException(e);
				}
			});
		} else {
			callback.onException(new Exception("Can't approve file for commit"));
		}
	}

	@Override
	public void download(final String filePath, StreamForwarder<ByteBuf> consumer, ResultCallback<CompletionCallback> crutch) {
		// FIXME (arashev) SLAY DAT CRUTCH
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
	public void listFiles(ResultCallback<Set<String>> files) {
		fileSystem.listFiles(files);
	}

	@Override
	public void showAlive(ResultCallback<Set<ServerInfo>> alive) {
		logic.onShowAlive(alive);
	}

	@Override
	public void checkOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
		logic.onOffer(forUpload, forDeletion, result);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void delete(String filePath) {
		fileSystem.deleteFile(filePath, ignoreCompletionCallback());
	}

	@Override
	public void replicate(final String filePath, final ServerInfo server) {
		// FIXME (arashev) don't like scheme with forwarder
		StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		fileSystem.get(filePath, forwarder);
		protocol.upload(server, filePath, forwarder, new CompletionCallback() {
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
	public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
		protocol.offer(server, forUpload, forDeletion, result);
	}

	@Override
	public void updateServerMap(final Set<ServerInfo> bootstrap, final ResultCallback<Set<ServerInfo>> callback) {
		for (ServerInfo server : bootstrap) {
			protocol.alive(server, new ResultCallback<Set<ServerInfo>>() {
				@Override
				public void onResult(Set<ServerInfo> result) {
					callback.onResult(result);
				}

				@Override
				public void onException(Exception ignore) {
					// Can't do anything. Bootstrap server doesn't answer.
				}
			});
		}
		eventloop.schedule(eventloop.currentTimeMillis() + SERVER_UPDATE_TIME, new Runnable() {
			@Override
			public void run() {
				updateServerMap(bootstrap, callback);
			}
		});
	}

	@Override
	public void scheduleTemporaryFileDeletion(final String filePath) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + TIME_TO_SLAY, new Runnable() {
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
	public void updateSystem() {
		eventloop.schedule(eventloop.currentTimeMillis() + TIMEOUT_TO_UPDATE, new Runnable() {
			@Override
			public void run() {
				logic.update();
			}
		});
	}
}
