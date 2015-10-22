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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.RUNNING;
import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.SHUTDOWN;

public class SimpleFsServer extends AbstractNioServer<SimpleFsServer> implements NioService {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServer.class);
	private static final long TIMEOUT = 10 * 1000;

	private static final String IN_PROGRESS_EXTENSION = ".partial";
	private static final String TMP_DIRECTORY = "tmp";

	private final ExecutorService executor;

	private int pendingOperationsCounter;
	private final Set<String> filesToBeCommited = new HashSet<>();
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
			cleanFolder(tmpStorage);
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
				final String fileName = item.fileName;
				logger.info("Server received command to upload file {}", fileName);

				if (serverStatus != RUNNING) {
					refuse(messaging, "Server is being shut down");
					return;
				}

				Path destination;
				final Path inProgress;
				try {
					destination = getDestinationDirectory(fileName);
					inProgress = getInProgressDirectory(fileName);
				} catch (IOException e) {
					logger.error("Can't create directory for file {}", fileName, e);
					refuse(messaging, "Can't create directory for file");
					return;
				}

				if (Files.exists(destination) || Files.exists(inProgress)) {
					refuse(messaging, "File already exists");
					return;
				}

				messaging.sendMessage(new SimpleFsResponseOperationOk());
				startOperation();

				logger.trace("Starting uploading file {}", fileName);
				StreamProducer<ByteBuf> producer = messaging.read();
				StreamFileWriter diskWrite = StreamFileWriter.createFile(eventloop, executor, inProgress, true);
				producer.streamTo(diskWrite);

				diskWrite.setFlushCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.trace("Uploaded file {}", fileName);
						filesToBeCommited.add(fileName);
						eventloop.scheduleBackground(eventloop.currentTimeMillis() + TIMEOUT, new Runnable() {
							@Override
							public void run() {
								if (filesToBeCommited.contains(fileName)) {
									try {
										filesToBeCommited.remove(fileName);
										Files.delete(inProgress);
										operationFinished();
										logger.info("Deleted uploaded but not approved for commit file {}", fileName);
									} catch (IOException e) {
										logger.error("Can't delete uploaded but not approved for commit file {}", fileName, e);
									}
								}
							}
						});
						messaging.sendMessage(new SimpleFsResponseAcknowledge());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't upload file {}", fileName, e);
						operationFinished();
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
				final String fileName = item.fileName;
				logger.info("Server received command to " + (item.isOk ? "commit" : "cancel upload") + " file {}", fileName);

				if (serverStatus != RUNNING && pendingOperationsCounter <= 0) {
					refuse(messaging, "Server is being shut down");
					return;
				}

				Path destination;
				Path inProgress;
				try {
					destination = getDestinationDirectory(fileName);
					inProgress = getInProgressDirectory(fileName);
				} catch (IOException e) {
					logger.error("Can't create directory for file {}", fileName);
					refuse(messaging, "Can't create directory for file");
					return;
				}

				filesToBeCommited.remove(fileName);

				if (item.isOk) {
					try {
						Files.move(inProgress, destination);
						messaging.sendMessage(new SimpleFsResponseOperationOk());
						logger.info("File {} commited", fileName);
					} catch (IOException e) {
						try {
							Files.delete(inProgress);
							logger.trace("Temporary file {} removed (impossible to commit)", inProgress.toAbsolutePath());
						} catch (IOException e1) {
							logger.trace("Can't remove temporary file {} (impossible to commit) ", inProgress.toAbsolutePath());
						}
						messaging.sendMessage(new SimpleFsResponseError(e.getMessage()));
					}
				} else {
					try {
						Files.delete(inProgress);
						messaging.sendMessage(new SimpleFsResponseOperationOk());
						logger.trace("Temporary file {} removed (Exception on client side)", inProgress.toAbsolutePath());
					} catch (IOException e) {
						logger.trace("Can't remove temporary file {} (impossible to commit) ", inProgress.toAbsolutePath());
					}
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
				});

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
					deleteFile(path);
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

	private void deleteFile(Path path) throws IOException {
		if (Files.isDirectory(path)) {
			if (isDirEmpty(path)) {
				Files.delete(path);
			}
			path = path.getParent();
			if (path != null && !path.equals(fileStorage)) {
				deleteFile(path);
			}
		} else {
			Files.delete(path);
			deleteFile(path.getParent());
		}
	}

	private boolean isDirEmpty(final Path directory) throws IOException {
		try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
			return !dirStream.iterator().hasNext();
		}
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
					List<String> fileList = new ArrayList<>();
					listFiles(fileStorage, fileList);
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

	private Path getDestinationDirectory(String path) throws IOException {
		return getDirectory(fileStorage, path);
	}

	private Path getInProgressDirectory(String path) throws IOException {
		return getDirectory(tmpStorage, path + IN_PROGRESS_EXTENSION);
	}

	private Path getDirectory(Path container, String path) throws IOException {
		String fileName = getFileName(path);
		String filePath = getFilePath(path);

		Path destinationDirectory = container.resolve(filePath);

		Files.createDirectories(destinationDirectory);
		logger.trace("Resolved directory for path {}", filePath);

		return destinationDirectory.resolve(fileName);
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

	private void cleanFolder(Path container) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(container)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path) && path.iterator().hasNext()) {
					cleanFolder(path);
				} else {
					Files.delete(path);
				}
			}
		}
	}

	private void listFiles(Path container, List<String> fileNames) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(container)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					if (!path.equals(tmpStorage)) {
						listFiles(path, fileNames);
					}
				} else {
					fileNames.add(path.getFileName().toString());
				}
			}
		}
	}

	private String getFileName(String path) {
		if (path.contains(File.separator)) {
			path = path.substring(path.lastIndexOf(File.separator) + 1);
		}
		return path;
	}

	private String getFilePath(String path) {
		if (path.contains(File.separator)) {
			path = path.substring(0, path.lastIndexOf(File.separator));
		} else {
			path = "";
		}
		return path;
	}
}