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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.RUNNING;
import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.SHUTDOWN;

public class SimpleFsServer extends AbstractNioServer<SimpleFsServer> implements NioService {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServer.class);

	private static final String IN_PROGRESS_EXTENSION = ".partial";
	private static final String TMP_DIRECTORY = "tmp";

	private final ExecutorService executor;

	private int pendingOperationsCounter;
	private CompletionCallback callbackOnStop;

	private final Path fileStorage;
	private final Path tmpStorage;
	private final int bufferSize;

	private ServerStatus serverStatus;

	enum ServerStatus {
		RUNNING, SHUTDOWN
	}

	private SimpleFsServer(final NioEventloop eventloop, final Path fileStorage, final Path tmpStorage, ExecutorService executor, int bufferSize) {
		super(eventloop);
		this.fileStorage = fileStorage;
		this.tmpStorage = tmpStorage;
		this.executor = executor;
		this.bufferSize = bufferSize;
	}

	public static SimpleFsServer createServer(final NioEventloop eventloop, final Path fileStorage, ExecutorService executor) {
		return createServer(eventloop, fileStorage, executor, 256 * 1024);
	}

	public static SimpleFsServer createServer(final NioEventloop eventloop, final Path fileStorage, final Path tmpStorage, ExecutorService executor) {
		return new SimpleFsServer(eventloop, fileStorage, tmpStorage, executor, 256 * 1024);
	}

	public static SimpleFsServer createServer(final NioEventloop eventloop, final Path fileStorage, ExecutorService executor, int bufferSize) {
		Path tmpStorage = fileStorage.resolve(TMP_DIRECTORY);
		return new SimpleFsServer(eventloop, fileStorage, tmpStorage, executor, bufferSize);
	}

	@Override
	public void start(CompletionCallback callback) {
		logger.info("Starting SimpleFS");

		if (serverStatus == RUNNING) {
			callback.onComplete();
			return;
		}

		try {
			ensureInfrastructure();
			clearFolder(tmpStorage);
			serverStatus = RUNNING;
			logger.trace("Started SimpleFS");
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Failed to start SimpleFS");
			callback.onException(e);
		}
	}

	@Override
	public void stop(final CompletionCallback callback) {
		logger.info("Stopping SimpleFS");
		serverStatus = SHUTDOWN;
		if (pendingOperationsCounter == 0) {
			callback.onComplete();
			self().close();
			return;
		}
		callbackOnStop = callback;
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 10),
				new StreamGsonSerializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 256 * 1024, 256 * (1 << 20), 0))
				.addHandler(SimpleFsCommandUpload.class, defineUploadHandler())
				.addHandler(SimpleFsCommandCommit.class, defineCommitHandler())
				.addHandler(SimpleFsCommandDownload.class, defineDownloadHandler())
				.addHandler(SimpleFsCommandDelete.class, defineDeleteHandler())
				.addHandler(SimpleFsCommandList.class, defineListHandler());
	}

	private MessagingHandler<SimpleFsCommandUpload, SimpleFsResponse> defineUploadHandler() {
		return new MessagingHandler<SimpleFsCommandUpload, SimpleFsResponse>() {
			@Override
			public void onMessage(final SimpleFsCommandUpload item, final Messaging<SimpleFsResponse> messaging) {
				final String fileName = getFileName(item.fileName);
				logger.info("Server received command to upload file {}", fileName);

				if (serverStatus != RUNNING) {
					refuse(messaging, "Server is being shut down");
					return;
				}

				startOperation();

				Path destination = fileStorage.resolve(fileName);
				Path inProgress = tmpStorage.resolve(fileName + IN_PROGRESS_EXTENSION);

				if (Files.exists(destination) || Files.exists(inProgress)) {
					refuse(messaging, "File already exists");
					return;
				}

				messaging.sendMessage(new SimpleFsResponseOperationOk());

				logger.trace("Starting uploading file {}", fileName);
				StreamProducer<ByteBuf> producer = messaging.binarySocketReader();
				StreamFileWriter diskWrite = StreamFileWriter.createFile(eventloop, executor, inProgress, true);
				producer.streamTo(diskWrite);

				diskWrite.setFlushCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.trace("Uploaded file {}", fileName);
						messaging.sendMessage(new SimpleFsResponseAcknowledge());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't upload file {}", fileName, e);
						messaging.sendMessage(new SimpleFsResponseError("Can't upload file: " + e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<SimpleFsCommandCommit, SimpleFsResponse> defineCommitHandler() {
		return new MessagingHandler<SimpleFsCommandCommit, SimpleFsResponse>() {
			@Override
			public void onMessage(SimpleFsCommandCommit item, Messaging<SimpleFsResponse> messaging) {
				final String fileName = getFileName(item.fileName);
				logger.info("Server received command to commit file {}", fileName);

				if (serverStatus != RUNNING && pendingOperationsCounter > 0) {
					refuse(messaging, "Server is being shut down");
					return;
				}

				final Path destination = fileStorage.resolve(fileName);
				final Path inProgress = tmpStorage.resolve(fileName + IN_PROGRESS_EXTENSION);
				try {
					Files.move(inProgress, destination);
					messaging.sendMessage(new SimpleFsResponseOperationOk());
					logger.info("File {} commited", fileName);
				} catch (IOException e) {
					try {
						Files.delete(inProgress);
						logger.trace("Temporary file {} removed(impossible to commit)", inProgress.toAbsolutePath());
					} catch (IOException e1) {
						logger.trace("Can't remove temporary file {} (impossible to commit) ", inProgress.toAbsolutePath());
					}
					messaging.sendMessage(new SimpleFsResponseError(e.getMessage()));
				}
				operationFinished();
				messaging.shutdown();
			}
		};
	}

	private MessagingHandler<SimpleFsCommandDownload, SimpleFsResponse> defineDownloadHandler() {
		return new MessagingHandler<SimpleFsCommandDownload, SimpleFsResponse>() {
			@Override
			public void onMessage(SimpleFsCommandDownload item, final Messaging<SimpleFsResponse> messaging) {
				final String fileName = item.fileName;
				logger.info("Server received command to download file {}", fileName);

				if (serverStatus != RUNNING) {
					refuse(messaging, "Server is being shut down");
					return;
				}

				Path source = fileStorage.resolve(fileName);
				if (!Files.exists(source)) {
					refuse(messaging, "File not found");
					return;
				}
				startOperation();
				messaging.sendMessage(new SimpleFsResponseOperationOk());

				StreamProducer<ByteBuf> producer = StreamFileReader.readFileFrom(eventloop, executor, bufferSize, source, 0L);

				messaging.write(producer, new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.trace("File {} send", fileName);
								operationFinished();
							}

							@Override
							public void onException(Exception e) {
								logger.error("File {} was not send", fileName, e);
								operationFinished();
							}
						}

				);
				messaging.shutdownReader();
			}

		};
	}

	private MessagingHandler<SimpleFsCommandDelete, SimpleFsResponse> defineDeleteHandler() {
		return new MessagingHandler<SimpleFsCommandDelete, SimpleFsResponse>() {
			@Override
			public void onMessage(SimpleFsCommandDelete item, Messaging<SimpleFsResponse> messaging) {
				String fileName = item.fileName;
				logger.info("Server received command to delete file: {}", fileName);

				if (serverStatus != RUNNING) {
					refuse(messaging, "Server is being shut down");
					return;
				}

				Path path = fileStorage.resolve(fileName);
				if (!Files.exists(path)) {
					refuse(messaging, "File not found");
					return;
				}

				try {
					Files.delete(path);
					messaging.sendMessage(new SimpleFsResponseOperationOk());
					logger.info("File {} deleted", fileName);
				} catch (IOException e) {
					messaging.sendMessage(new SimpleFsResponseError("Can't delete file: " + e.getMessage()));
					logger.error("Can't delete file: {}", fileName, e);
				}
				messaging.shutdown();
			}
		};
	}

	private MessagingHandler<SimpleFsCommandList, SimpleFsResponse> defineListHandler() {
		return new MessagingHandler<SimpleFsCommandList, SimpleFsResponse>() {
			@Override
			public void onMessage(SimpleFsCommandList item, Messaging<SimpleFsResponse> messaging) {
				logger.info("Server received command to list files");

				if (serverStatus != RUNNING) {
					refuse(messaging, "Server is being shut down");
					return;
				}

				try {
					List<String> fileList = listFiles();
					messaging.sendMessage(new SimpleFsResponseFileList(fileList));
					logger.info("Send list(size={}) of files", fileList.size());
				} catch (IOException e) {
					messaging.sendMessage(new SimpleFsResponseError("Can't get list of files: " + e.getMessage()));
					logger.error("Can't get list of files");
				}
				messaging.shutdown();
			}
		};
	}

	private void refuse(Messaging<SimpleFsResponse> messaging, String msg) {
		logger.info("Refused: " + msg);
		messaging.sendMessage(new SimpleFsResponseError(msg));
		messaging.shutdown();
	}

	private void startOperation() {
		pendingOperationsCounter++;
	}

	private void operationFinished() {
		pendingOperationsCounter--;
		if (serverStatus == SHUTDOWN && pendingOperationsCounter == 0) {
			if (callbackOnStop != null) {
				callbackOnStop.onComplete();
			}
			self().close();
		}
	}

	private void ensureInfrastructure() throws IOException {
		Files.createDirectories(tmpStorage);
	}

	private void clearFolder(Path folder) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(folder)) {
			for (Path path : directoryStream) {
				Files.delete(path);
			}
		}
	}

	private List<String> listFiles() throws IOException {
		List<String> fileNames = new ArrayList<>();
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(fileStorage)) {
			for (Path path : directoryStream) {
				if (!Files.isDirectory(path))
					fileNames.add(path.getFileName().toString());
			}
		}
		return fileNames;
	}

	private String getFileName(String path) {
		if (path.contains(File.separator)) {
			path = path.substring(path.lastIndexOf(File.separator) + 1);
		}
		return path;
	}
}
