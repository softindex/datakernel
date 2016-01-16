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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
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
import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.RUNNING;
import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.SHUTDOWN;

@SuppressWarnings("unused")
public final class SimpleFsServer implements EventloopService {
	public static final class Builder {
		private final GsonServerProtocol.Builder protocolBuilder;
		private final Eventloop eventloop;

		private final List<InetSocketAddress> addresses = new ArrayList<>();

		private long approveWaitTime = DEFAULT_APPROVE_WAIT_TIME;

		private ExecutorService executor;
		private Path storage;
		private Path tmpStorage;

		private int readerBufferSize = FileSystem.DEFAULT_READER_BUFFER_SIZE;
		private String inProgressExtension = FileSystem.DEFAULT_IN_PROGRESS_EXTENSION;

		public Builder(Eventloop eventloop, ExecutorService executor, Path storage, Path tmpStorage) {
			this.eventloop = eventloop;
			this.executor = executor;

			if (storage.startsWith(tmpStorage) || tmpStorage.startsWith(storage)) {
				throw new IllegalStateException("TmpStorage and storage must not be related directories(parent-child like)");
			}

			this.storage = storage;
			this.tmpStorage = tmpStorage;
			protocolBuilder = GsonServerProtocol.buildInstance(eventloop);
		}

		public Builder setApproveWaitTime(long approveWaitTime) {
			this.approveWaitTime = approveWaitTime;
			return this;
		}

		public Builder setListenAddress(InetSocketAddress address) {
			this.addresses.add(address);
			return this;
		}

		public Builder setListenAddresses(List<InetSocketAddress> addresses) {
			this.addresses.addAll(addresses);
			return this;
		}

		public Builder setListenPort(int port) {
			this.addresses.add(new InetSocketAddress(port));
			return this;
		}

		public Builder setInProgressExtension(String inProgressExtension) {
			this.inProgressExtension = inProgressExtension;
			return this;
		}

		public Builder setReaderBufferSize(int readerBufferSize) {
			this.readerBufferSize = readerBufferSize;
			return this;
		}

		public Builder setDeserializerBufferSize(int deserializerBufferSize) {
			protocolBuilder.setDeserializerBufferSize(deserializerBufferSize);
			return this;
		}

		public Builder setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			protocolBuilder.setSerializerFlushDelayMillis(serializerFlushDelayMillis);
			return this;
		}

		public Builder setSerializerBufferSize(int serializerBufferSize) {
			protocolBuilder.setSerializerBufferSize(serializerBufferSize);
			return this;
		}

		public Builder setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			protocolBuilder.setSerializerMaxMessageSize(serializerMaxMessageSize);
			return this;
		}

		public SimpleFsServer build() {
			FileSystem fs = FileSystem.newInstance(eventloop, executor, storage, tmpStorage,
					readerBufferSize, inProgressExtension);
			GsonServerProtocol protocol = protocolBuilder.build();
			SimpleFsServer server = new SimpleFsServer(eventloop, fs, protocol, approveWaitTime);
			protocol.wireServer(server);
			protocol.setListenAddresses(addresses);
			return server;
		}
	}

	public static final long DEFAULT_APPROVE_WAIT_TIME = 10 * 100;
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	public static final Exception SERVER_IS_DOWN_EXCEPTION = new Exception("Server is down");

	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServer.class);
	private final Eventloop eventloop;

	private final FileSystem fileSystem;
	private final GsonServerProtocol protocol;

	private final Set<String> filesToBeCommited = new HashSet<>();
	private final long approveWaitTime;

	private CompletionCallback callbackOnStop;
	private ServerStatus serverStatus;

	private SimpleFsServer(Eventloop eventLoop, FileSystem fileSystem, GsonServerProtocol protocol, long approveWaitTime) {
		this.eventloop = eventLoop;
		this.fileSystem = fileSystem;
		this.protocol = protocol;
		this.approveWaitTime = approveWaitTime;
	}

	public static SimpleFsServer createInstance(Eventloop eventloop, ExecutorService executor, Path storage,
	                                            Path tmpStorage, List<InetSocketAddress> addresses) {
		return buildInstance(eventloop, executor, storage, tmpStorage)
				.setListenAddresses(addresses)
				.build();
	}

	public static SimpleFsServer createInstance(Eventloop eventloop, ExecutorService executor, Path storage,
	                                            Path tmpStorage, InetSocketAddress address) {
		return buildInstance(eventloop, executor, storage, tmpStorage)
				.setListenAddress(address)
				.build();
	}

	public static SimpleFsServer createInstance(Eventloop eventloop, ExecutorService executor, Path storage,
	                                            Path tmpStorage, int port) {
		return buildInstance(eventloop, executor, storage, tmpStorage)
				.setListenPort(port)
				.build();
	}

	public static Builder buildInstance(Eventloop eventloop, ExecutorService executor, Path storage, Path tmpStorage) {
		return new Builder(eventloop, executor, storage, tmpStorage);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		logger.info("Starting SimpleFS");

		if (serverStatus == RUNNING) {
			callback.onComplete();
			return;
		}

		try {
			protocol.listen();
			fileSystem.initDirectories();
			serverStatus = RUNNING;
			callback.onComplete();
		} catch (IOException e) {
			callback.onException(e);
		}

	}

	@Override
	public void stop(final CompletionCallback callback) {
		logger.info("Stopping SimpleFS");
		serverStatus = SHUTDOWN;
		if (filesToBeCommited.isEmpty()) {
			protocol.close();
			callback.onComplete();
		} else {
			callbackOnStop = callback;
		}
	}

	void upload(final String fileName, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		logger.info("Received command to upload file: {}", fileName);

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(SERVER_IS_DOWN_EXCEPTION);
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

	void commit(final String fileName, boolean success, final CompletionCallback callback) {
		logger.info("Received command to commit file: {}, {}", fileName, success);

		if (serverStatus != RUNNING && !filesToBeCommited.contains(fileName)) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(SERVER_IS_DOWN_EXCEPTION);
			return;
		}

		filesToBeCommited.remove(fileName);

		if (success) {
			fileSystem.commitTmp(fileName, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Commited file: {}", fileName);
					callback.onComplete();
					onOperationFinished();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't commit file: {}", fileName, e);
					callback.onException(e);
					onOperationFinished();
				}
			});
		} else {
			fileSystem.deleteTmp(fileName, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Cancel commit file: {}", fileName);
					callback.onComplete();
					onOperationFinished();
				}

				@Override
				public void onException(Exception e) {
					logger.error("Can't cancel commit file: {}", fileName, e);
					callback.onException(e);
					onOperationFinished();
				}
			});
		}
	}

	long size(String fileName) {
		return fileSystem.exists(fileName);
	}

	StreamProducer<ByteBuf> download(String fileName, long startPosition) {
		logger.info("Received command to download file: {}, start position: {}", fileName, startPosition);

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			return StreamProducers.closingWithError(eventloop, SERVER_IS_DOWN_EXCEPTION);
		}

		return fileSystem.get(fileName, startPosition);
	}

	void delete(String fileName, CompletionCallback callback) {
		logger.info("Received command to delete file: {}", fileName);

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(SERVER_IS_DOWN_EXCEPTION);
			return;
		}

		fileSystem.delete(fileName, callback);
	}

	void list(ResultCallback<Set<String>> callback) {
		logger.info("Received command to list files");

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			callback.onException(SERVER_IS_DOWN_EXCEPTION);
			return;
		}

		fileSystem.list(callback);
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

	private void onOperationFinished() {
		if (serverStatus == SHUTDOWN && filesToBeCommited.isEmpty()) {
			protocol.close();
			callbackOnStop.onComplete();
		}
	}

	enum ServerStatus {
		RUNNING, SHUTDOWN
	}
}