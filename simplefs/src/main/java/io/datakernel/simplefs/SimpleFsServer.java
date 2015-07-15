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
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.stream.StreamConsumer;
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

public class SimpleFsServer extends AbstractNioServer<SimpleFsServer> {

	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServer.class);
	private static final String IN_PROGRESS_EXTENSION = ".partial";
	private final ExecutorService executor;
	private final Path fileStorage;
	private final int bufferSize;

	private SimpleFsServer(final NioEventloop eventloop, final Path fileStorage, ExecutorService executor, int bufferSize) {
		super(eventloop);
		this.fileStorage = fileStorage;
		this.executor = executor;
		this.bufferSize = bufferSize;
	}

	public static SimpleFsServer createServer(final NioEventloop eventloop, final Path fileStorage, ExecutorService executor) throws IOException {
		return createServer(eventloop, fileStorage, executor, 256 * 1024);
	}

	public static SimpleFsServer createServer(final NioEventloop eventloop, final Path fileStorage, ExecutorService executor, int bufferSize) throws IOException {
		SimpleFsServer server = new SimpleFsServer(eventloop, fileStorage, executor, bufferSize);
		Files.createDirectories(fileStorage);
		// Delete not uploaded files.
		List<String> files = server.fileList();
		for (String fileName : files) {
			if (fileName.endsWith(IN_PROGRESS_EXTENSION)) {
				Path destination = server.fileStorage.resolve(fileName);
				Files.delete(destination);
			}
		}

		logger.info("Start simple fs server");
		return server;
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 256 * 1024),
				new StreamGsonSerializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 256 * 1024, 256 * (1 << 20), 0))
				.addHandler(SimpleFsCommandDownload.class, new MessagingHandler<SimpleFsCommandDownload, SimpleFsResponse>() {
					@Override
					public void onMessage(final SimpleFsCommandDownload item, Messaging<SimpleFsResponse> messaging) {
						String fileName = item.filename;
						if (fileName.contains(File.separator)) {
							fileName = fileName.substring(fileName.lastIndexOf(File.separator) + 1);
						}

						logger.info("Server receive command download file {}", fileName);

						final Path source = fileStorage.resolve(fileName);

						if (!Files.exists(source)) {
							logger.info("Server not found file {}", fileName);
							messaging.sendMessage(new SimpleFsResponseError("File not found"));
							messaging.shutdown();
							return;
						}

						messaging.sendMessage(new SimpleFsResponseOperationOk());

						StreamFileReader.readFileFrom(eventloop, executor, bufferSize, source, 0L).streamTo(messaging.binarySocketWriter());
						messaging.shutdownReader();
					}
				})
				.addHandler(SimpleFsCommandUpload.class, new MessagingHandler<SimpleFsCommandUpload, SimpleFsResponse>() {
					@Override
					public void onMessage(final SimpleFsCommandUpload item, Messaging<SimpleFsResponse> messaging) {
						String fileName = item.filename;
						if (fileName.contains(File.separator)) {
							fileName = fileName.substring(fileName.lastIndexOf(File.separator) + 1);
						}

						logger.info("Server receive command upload file {}", fileName);

						final Path destination = fileStorage.resolve(fileName);
						final Path inProgress = fileStorage.resolve(fileName + IN_PROGRESS_EXTENSION);

						if (Files.exists(destination) || Files.exists(inProgress)) {
							logger.info("Client tries upload exists file {}", fileName);
							messaging.sendMessage(new SimpleFsResponseError("File already exists"));
							messaging.shutdown();
							return;
						}

						messaging.sendMessage(new SimpleFsResponseOperationOk());

						StreamProducer<ByteBuf> producer = messaging.binarySocketReader();
//						StreamLZ4Validator lz4DecompressorValidate = new StreamLZ4Validator(eventloop);
						StreamConsumer<ByteBuf> diskWrite = StreamFileWriter.createFile(eventloop, executor, inProgress, true);

						producer.streamTo(diskWrite);
//						producer.streamTo(lz4DecompressorValidate);
//						lz4DecompressorValidate.streamTo(diskWrite);

						diskWrite.addCompletionCallback(new CompletionCallback() {
							@Override
							public void onComplete() {
								// rename file
								try {
									Files.move(inProgress, destination);
								} catch (IOException e) {
									logger.error("Can't rename file {} to {}", inProgress.toAbsolutePath(), destination.toAbsolutePath(), e);
									try {
										Files.delete(inProgress);
									} catch (IOException e1) {
										logger.error("Can't remove file {} ", inProgress.toAbsolutePath(), e1);
									}
								}
							}

							@Override
							public void onException(Exception exception) {
								logger.error("Can't upload file {}", item.filename, exception);
							}
						});

						messaging.shutdownWriter();
					}

				})
				.addHandler(SimpleFsCommandList.class, new MessagingHandler<SimpleFsCommandList, SimpleFsResponse>() {
					@Override
					public void onMessage(SimpleFsCommandList item, Messaging<SimpleFsResponse> messaging) {
						try {
							List<String> fileList = fileList();
							messaging.sendMessage(new SimpleFsResponseFileList(fileList));
						} catch (IOException e) {
							messaging.sendMessage(new SimpleFsResponseError("Can't get list of files\t" + e.getMessage()));
						}

						messaging.shutdown();
					}
				})
				.addHandler(SimpleFsCommandDelete.class, new MessagingHandler<SimpleFsCommandDelete, SimpleFsResponse>() {
					@Override
					public void onMessage(SimpleFsCommandDelete item, Messaging<SimpleFsResponse> messaging) {
						Path path = fileStorage.resolve(item.fileName);

						if (!Files.exists(path)) {
							messaging.sendMessage(new SimpleFsResponseError("File not found for delete"));
						} else {
							try {
								Files.delete(path);
							} catch (IOException e) {
								messaging.sendMessage(new SimpleFsResponseError("Can't delete file\t" + e.getMessage()));
							}
						}
						messaging.shutdown();
					}
				});
	}

	private List<String> fileList() throws IOException {
		List<String> fileNames = new ArrayList<>();
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(fileStorage)) {
			for (Path path : directoryStream) {
				fileNames.add(path.getFileName().toString());
			}
		}
		return fileNames;
	}

}
