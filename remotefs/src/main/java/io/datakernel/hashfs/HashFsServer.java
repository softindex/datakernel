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

import com.google.gson.Gson;
import io.datakernel.FsResponses.ListOfFiles;
import io.datakernel.FsServer;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.net.MessagingWithBinaryStreamingConnection;

import java.util.List;
import java.util.Set;

import static io.datakernel.FsResponses.Err;
import static io.datakernel.FsResponses.FsResponse;
import static io.datakernel.hashfs.HashFsCommands.Alive;
import static io.datakernel.hashfs.HashFsCommands.Announce;
import static io.datakernel.hashfs.HashFsResponses.ListOfServers;

public final class HashFsServer extends FsServer<HashFsServer> {
	private final LocalReplica localReplica;

	// creators & builder methods
	public HashFsServer(Eventloop eventloop, LocalReplica localReplica) {
		super(eventloop, localReplica.getFileManager());
		this.localReplica = localReplica;
		this.handlers.put(Alive.class, new AliveMessagingHandler());
		this.handlers.put(Announce.class, new AnnounceMessagingHandler());
	}

	// core
	@Override
	protected final void upload(final String fileName, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		if (localReplica.canUpload(fileName)) {
			localReplica.onUploadStart(fileName);
			fileManager.save(fileName, new ResultCallback<StreamFileWriter>() {
				@Override
				public void onResult(StreamFileWriter writer) {
					logger.trace("{} opened", writer);
					writer.setFlushCallback(new ForwardingCompletionCallback(this) {
						@Override
						public void onComplete() {
							localReplica.onUploadComplete(fileName);
						}
					});
					callback.onResult(writer);
				}

				@Override
				public void onException(Exception e) {
					localReplica.onUploadFailed(fileName);
					callback.onException(e);
				}
			});
		} else {
			logger.warn("refused to upload {}", fileName);
			callback.onException(new Exception("Refused to upload file"));
		}
	}

	@Override
	protected final void download(final String fileName, final long startPosition, final ResultCallback<StreamProducer<ByteBuf>> callback) {
		if (localReplica.canDownload(fileName)) {
			localReplica.onDownloadStart(fileName);
			fileManager.get(fileName, startPosition, new ResultCallback<StreamFileReader>() {
				@Override
				public void onResult(StreamFileReader reader) {
					logger.trace("{} opened", reader);
					reader.setPositionCallback(new ForwardingResultCallback<Long>(this) {
						@Override
						public void onResult(Long result) {
							logger.trace("streamed {} bytes for {}", result - startPosition, fileName);
							localReplica.onDownloadComplete(fileName);
						}
					});
					callback.onResult(reader);
				}

				@Override
				public void onException(Exception e) {
					localReplica.onDownloadFailed(fileName);
					callback.onException(e);
				}
			});
		} else {
			logger.warn("refused to download {}", fileName);
			callback.onException(new Exception("Refused to download file"));
		}
	}

	@Override
	protected final void delete(final String fileName, final CompletionCallback callback) {
		if (localReplica.canDelete(fileName)) {
			localReplica.onDeletionStart(fileName);
			fileManager.delete(fileName, new CompletionCallback() {
				@Override
				public void onComplete() {
					localReplica.onDeleteComplete(fileName);
					callback.onComplete();
				}

				@Override
				public void onException(Exception e) {
					localReplica.onDeleteFailed(fileName);
					callback.onException(e);
				}
			});
		} else {
			logger.warn("refused to delete {}", fileName);
			callback.onException(new Exception("Refused to delete file"));
		}
	}

	@Override
	protected void list(ResultCallback<List<String>> callback) {
		localReplica.getList(callback);
	}

	@Override
	protected Gson getResponseGson() {
		return HashFsResponses.responseGSON;
	}

	@Override
	protected Gson getCommandGSON() {
		return HashFsCommands.commandGSON;
	}

	private class AliveMessagingHandler implements MessagingHandler<Alive, FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreamingConnection<Alive, FsResponse> messaging, Alive item) {
			localReplica.showAlive(eventloop.currentTimeMillis(), new ResultCallback<Set<Replica>>() {
				@Override
				public void onResult(Set<Replica> result) {
					messaging.send(new ListOfServers(result), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.send(new Err(e.getMessage()), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}
			});
		}
	}

	private class AnnounceMessagingHandler implements MessagingHandler<Announce, FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreamingConnection<Announce, FsResponse> messaging, Announce item) {
			localReplica.onAnnounce(item.forUpload, item.forDeletion, new ResultCallback<List<String>>() {
				@Override
				public void onResult(List<String> result) {
					messaging.send(new ListOfFiles(result), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.send(new Err(e.getMessage()), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}
			});
		}
	}
}