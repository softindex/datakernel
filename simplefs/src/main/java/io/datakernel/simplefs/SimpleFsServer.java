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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
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

public final class SimpleFsServer implements NioService {
	public static final class Builder {
		private final NioEventloop eventloop;
		private final FileSystem.Builder fsBuilder;
		private final GsonServerProtocol.Builder protocolBuilder;

		private final List<InetSocketAddress> addresses = new ArrayList<>();

		private long approveWaitTime = DEFAULT_APPROVE_WAIT_TIME;

		public Builder(NioEventloop eventloop, ExecutorService executor, Path storage) {
			this.eventloop = eventloop;
			fsBuilder = FileSystem.buildInstance(eventloop, executor, storage);
			protocolBuilder = GsonServerProtocol.buildInstance(eventloop);
		}

		public Builder setApproveWaitTime(long approveWaitTime) {
			this.approveWaitTime = approveWaitTime;
			return this;
		}

		public Builder specifyListenAddress(InetSocketAddress address) {
			this.addresses.add(address);
			return this;
		}

		public Builder specifyListenAddresses(List<InetSocketAddress> addresses) {
			this.addresses.addAll(addresses);
			return this;
		}

		public Builder specifyListenPort(int port) {
			this.addresses.add(new InetSocketAddress(port));
			return this;
		}

		public Builder setTmpStorage(Path tmpStorage) {
			fsBuilder.setTmpStorage(tmpStorage);
			return this;
		}

		public Builder setInProgressExtension(String inProgressExtension) {
			fsBuilder.setInProgressExtension(inProgressExtension);
			return this;
		}

		public Builder setReaderBufferSize(int readerBufferSize) {
			fsBuilder.setReaderBufferSize(readerBufferSize);
			return this;
		}

		public Builder setTmpDirectoryName(String tmpDirectoryName) {
			fsBuilder.setTmpDirectoryName(tmpDirectoryName);
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
			FileSystem fs = fsBuilder.build();
			GsonServerProtocol protocol = protocolBuilder.build();
			SimpleFsServer server = new SimpleFsServer(eventloop, fs, protocol, approveWaitTime);
			protocol.wireServer(server);
			protocol.setListenAddresses(addresses);
			return server;
		}
	}

	public static final long DEFAULT_APPROVE_WAIT_TIME = 10 * 100;
	public static final Exception SERVER_IS_DOWN_EXCEPTION = new Exception("Server is down");

	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServer.class);
	private final NioEventloop eventloop;

	private final FileSystem fileSystem;
	private final GsonServerProtocol protocol;

	private final Set<String> filesToBeCommited = new HashSet<>();
	private final long approveWaitTime;

	private CompletionCallback callbackOnStop;
	private ServerStatus serverStatus;

	private SimpleFsServer(NioEventloop eventLoop, FileSystem fileSystem, GsonServerProtocol protocol, long approveWaitTime) {
		this.eventloop = eventLoop;
		this.fileSystem = fileSystem;
		this.protocol = protocol;
		this.approveWaitTime = approveWaitTime;
	}

	public static SimpleFsServer createInstance(NioEventloop eventloop, ExecutorService executor, Path storage,
	                                            List<InetSocketAddress> addresses) {
		return buildInstance(eventloop, executor, storage)
				.specifyListenAddresses(addresses)
				.build();
	}

	public static SimpleFsServer createInstance(NioEventloop eventloop, ExecutorService executor, Path storage,
	                                            InetSocketAddress address) {
		return buildInstance(eventloop, executor, storage)
				.specifyListenAddress(address)
				.build();
	}

	public static SimpleFsServer createInstance(NioEventloop eventloop, ExecutorService executor, Path storage,
	                                            int port) {
		return buildInstance(eventloop, executor, storage)
				.specifyListenPort(port)
				.build();
	}

	public static Builder buildInstance(NioEventloop eventloop, ExecutorService executor, Path storage) {
		return new Builder(eventloop, executor, storage);
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

		try {
			protocol.listen();
			fileSystem.ensureInfrastructure();
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

	StreamProducer<ByteBuf> download(final String fileName) {
		logger.info("Received command to download file: {}", fileName);

		if (serverStatus != RUNNING) {
			logger.info("Can't perform operation. Server is down!");
			return StreamProducers.closingWithError(eventloop, SERVER_IS_DOWN_EXCEPTION);
		}

		return fileSystem.get(fileName);
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