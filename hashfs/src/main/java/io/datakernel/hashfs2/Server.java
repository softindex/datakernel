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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Server {
	private final NioEventloop eventloop;
	private final Logic logic;
	private final FileSystem fileSystem;
	private final Client client;

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public Server(NioEventloop eventloop, Logic logic, FileSystem fileSystem, Client client) {
		this.eventloop = eventloop;
		this.logic = logic;
		this.fileSystem = fileSystem;
		this.client = client;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public void upload(final String filePath, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		if (logic.canUpload(filePath)) {
			logic.onUploadStart(filePath);
			fileSystem.stash(filePath, producer, new CompletionCallback() {
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

	public void commit(final String filePath, final boolean success, final CompletionCallback callback) {
		if (logic.canApprove(filePath)) {
			fileSystem.commit(filePath, success, new CompletionCallback() {
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

	public void listFile(ResultCallback<List<String>> files) {
		fileSystem.listFiles(files);
	}

	public void showAlive(ResultCallback<Set<ServerInfo>> alive) {
		logic.onShowAlive(alive);
	}

	public void checkOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
		logic.onOffer(forUpload, forDeletion, result);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	void replicate(final String filePath, final ServerInfo server) {
		// FIXME (arashev) don't like scheme with forwarder
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

	void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
		client.offer(server, forUpload, forDeletion, result);
	}

	void updateServerMap(List<ServerInfo> bootstrap, ResultCallback<Set<ServerInfo>> result) {
		final Set<ServerInfo> alive = new HashSet<>();
		for (ServerInfo server : bootstrap) {
			client.alive(server, new ResultCallback<Set<ServerInfo>>() {
				@Override
				public void onResult(Set<ServerInfo> result) {
					alive.addAll(result);
				}

				@Override
				public void onException(Exception ignore) {
					// Can't do anything. Bootstrap server doesn't answer.
				}
			});
		}
		result.onResult(alive);
	}
}
