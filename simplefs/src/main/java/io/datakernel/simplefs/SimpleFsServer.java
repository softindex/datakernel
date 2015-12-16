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

package io.datakernel.simplefs;

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.remotefs.*;
import io.datakernel.remotefs.protocol.ServerProtocol;
import io.datakernel.remotefs.protocol.gson.GsonServerProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.remotefs.FsServer.ServerStatus.RUNNING;
import static io.datakernel.remotefs.FsServer.ServerStatus.SHUTDOWN;

public final class SimpleFsServer implements FsServer {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServer.class);
	private final NioEventloop eventloop;

	private final FileSystem fileSystem;
	private ServerProtocol protocol;

	private final Set<String> filesToBeCommited = new HashSet<>();
	private CompletionCallback callbackOnStop;
	private final long approveWaitTime;

	private ServerStatus serverStatus;

	private SimpleFsServer(NioEventloop eventLoop, FileSystem fileSystem, ServerProtocol protocol, long approveWaitTime) {
		this.eventloop = eventLoop;
		this.fileSystem = fileSystem;
		this.protocol = protocol;
		this.approveWaitTime = approveWaitTime;
	}

	public static FsServer createInstance(NioEventloop eventLoop, FileSystem fileSystem, ServerProtocol protocol, RfsConfig config) {
		SimpleFsServer server = new SimpleFsServer(eventLoop, fileSystem, protocol, config.getApproveWaitTime());
		protocol.wireServer(server);
		return server;
	}

	public static FsServer createInstance(NioEventloop eventloop, ExecutorService executor, Path storage, int port) {
		return createInstance(eventloop, executor, storage, new InetSocketAddress(port));
	}

	public static FsServer createInstance(NioEventloop eventloop, ExecutorService executor, Path storage, InetSocketAddress address) {
		return createInstance(eventloop, executor, storage, Collections.singletonList(address));
	}

	public static FsServer createInstance(NioEventloop eventloop, ExecutorService executor, Path storage, List<InetSocketAddress> adresses) {
		RfsConfig config = RfsConfig.getDefaultConfig();
		FileSystem fileSystem = FileSystemImpl.createInstance(eventloop, executor, storage, config);
		ServerProtocol protocol = GsonServerProtocol.createInstance(eventloop, config, adresses);
		return createInstance(eventloop, fileSystem, protocol, config);
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		logger.info("Starting SimpleFS");

		if (serverStatus == RUNNING) {
			callback.onComplete();
			return;
		}

		CompletionCallback waiter = AsyncCallbacks.waitAll(2, new CompletionCallback() {
			@Override
			public void onComplete() {
				serverStatus = RUNNING;
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
		fileSystem.start(waiter);
		protocol.start(waiter);
	}

	@Override
	public void stop(final CompletionCallback callback) {
		logger.info("Stopping SimpleFS");
		serverStatus = SHUTDOWN;
		if (filesToBeCommited.isEmpty()) {
			CompletionCallback waiter = AsyncCallbacks.waitAll(2, callback);
			fileSystem.stop(waiter);
			protocol.stop(waiter);
		} else {
			callbackOnStop = callback;
		}
	}

	@Override
	public void upload(final String fileName, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		logger.info("Received command to upload file: {}", fileName);

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(new Exception("Server is down"));
			return;
		}

		fileSystem.saveToTmp(fileName, producer, new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				filesToBeCommited.add(fileName);
				scheduleTmpFileDeletion(fileName);
				callback.onComplete();
			}
		});
	}

	@Override
	public void commit(final String fileName, boolean success, final CompletionCallback callback) {
		logger.info("Received command to commit file: {}, {}", fileName, success);

		if (serverStatus != RUNNING && !filesToBeCommited.contains(fileName)) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(new Exception("Server is down"));
			return;
		}

		filesToBeCommited.remove(fileName);

		if (success) {
			fileSystem.commitTmp(fileName, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Commited file: {}", fileName);
					callback.onComplete();
					operationFinished();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't commit file: {}", fileName, e);
					callback.onException(e);
					operationFinished();
				}
			});
		} else {
			fileSystem.deleteTmp(fileName, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Cancel commit file: {}", fileName);
					callback.onComplete();
					operationFinished();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't cancel commit file: {}", fileName, e);
					callback.onException(e);
					operationFinished();
				}
			});
		}
	}

	@Override
	public void download(final String fileName, final StreamConsumer<ByteBuf> consumer, ResultCallback<CompletionCallback> callback) {
		logger.info("Received command to download file: {}", fileName);

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(new Exception("Server is down"));
			return;
		}

		fileSystem.get(fileName).streamTo(consumer);
		callback.onResult(ignoreCompletionCallback());
	}

	@Override
	public void delete(String fileName, CompletionCallback callback) {
		logger.info("Received command to delete file: {}", fileName);

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(new Exception("Server is down"));
			return;
		}

		fileSystem.delete(fileName, callback);
	}

	@Override
	public void list(ResultCallback<Set<String>> callback) {
		logger.info("Received command to list files");

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(new Exception("Server is down"));
			return;
		}

		fileSystem.list(callback);
	}

	@Override
	public void showAlive(ResultCallback<Set<ServerInfo>> callback) {
		callback.onException(new UnsupportedOperationException());
	}

	@Override
	public void checkOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		callback.onException(new UnsupportedOperationException());
	}

	private void scheduleTmpFileDeletion(final String fileName) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + approveWaitTime, new Runnable() {
			@Override
			public void run() {
				if (filesToBeCommited.contains(fileName)) {
					commit(fileName, false, ignoreCompletionCallback());
				}
			}
		});
	}

	private void operationFinished() {
		if (serverStatus == SHUTDOWN && filesToBeCommited.isEmpty()) {
			CompletionCallback waiter = AsyncCallbacks.waitAll(2, callbackOnStop);
			fileSystem.stop(waiter);
			protocol.stop(waiter);
		}
	}
}