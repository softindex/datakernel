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

package io.datakernel.logfs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static java.nio.file.StandardOpenOption.READ;

/**
 * Represents a file system for persisting logs. Stores files in a local file system.
 */
public final class LocalFsLogFileSystem extends AbstractLogFileSystem {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final ExecutorService executorService;
	private final Path dir;

	/**
	 * Constructs a log file system, that runs in the given event loop, runs blocking IO operations in the specified executor,
	 * stores logs in the given directory.
	 *  @param executorService executor for blocking IO operations
	 * @param dir             directory for storing log files
	 */
	private LocalFsLogFileSystem(ExecutorService executorService,
	                             Path dir) {
		this.executorService = executorService;
		this.dir = dir;
	}

	private LocalFsLogFileSystem(ExecutorService executorService, Path dir, String logName) {
		this.executorService = executorService;
		this.dir = dir.resolve(logName);
	}

	public static LocalFsLogFileSystem create(ExecutorService executorService, Path dir) {
		return new LocalFsLogFileSystem(executorService, dir);
	}

	public static LocalFsLogFileSystem create(ExecutorService executorService,
	                                          Path dir, String logName) {
		return new LocalFsLogFileSystem(executorService, dir, logName);
	}

	private Path path(String logPartition, LogFile logFile) {
		return dir.resolve(fileName(logPartition, logFile));
	}

	@Override
	public void list(final String logPartition, final ResultCallback<List<LogFile>> callback) {
		Eventloop eventloop = getCurrentEventloop();
		final Eventloop.ConcurrentOperationTracker concurrentOperationTracker = eventloop.startConcurrentOperation();
		try {
			executorService.execute(() -> {
				final List<LogFile> entries = new ArrayList<>();

				try {
					Files.createDirectories(dir);
					Files.walkFileTree(dir, new FileVisitor<Path>() {
						@Override
						public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
							PartitionAndFile partitionAndFile = parse(file.getFileName().toString());
							if (partitionAndFile != null && partitionAndFile.logPartition.equals(logPartition)) {
								entries.add(partitionAndFile.logFile);
							}
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
							if (exc != null) {
								logger.error("visitFileFailed error", exc);
							}
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
							if (exc != null) {
								logger.error("postVisitDirectory error", exc);
							}
							return FileVisitResult.CONTINUE;
						}
					});
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							callback.setResult(entries);
						}
					});
				} catch (final IOException e) {
					// TODO ?
					logger.error("walkFileTree error", e);
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							callback.setException(e);
						}
					});
				}

				concurrentOperationTracker.complete();
			});
		} catch (RejectedExecutionException e) {
			concurrentOperationTracker.complete();
			callback.setException(e);
		}
	}

	@Override
	public void read(String logPartition, LogFile logFile, final long startPosition, final StreamConsumer<ByteBuf> consumer) {
		Eventloop eventloop = getCurrentEventloop();
		AsyncFile.open(eventloop, executorService, path(logPartition, logFile), new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
			@Override
			protected void onResult(AsyncFile file) {
				StreamFileReader fileReader = StreamFileReader.readFileFrom(eventloop, file, 1024 * 1024, startPosition);
				fileReader.streamTo(consumer);
			}

			@Override
			protected void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
			}
		});
	}

	@Override
	public void write(String logPartition, LogFile logFile, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		Eventloop eventloop = getCurrentEventloop();
		AsyncFile.open(eventloop, executorService, path(logPartition, logFile), StreamFileWriter.CREATE_OPTIONS, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			protected void onResult(AsyncFile file) {
				StreamFileWriter fileWriter = StreamFileWriter.create(eventloop, file, true);
				producer.streamTo(fileWriter);
				fileWriter.setFlushCallback(callback);
			}
		});
	}
}
