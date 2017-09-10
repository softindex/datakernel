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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static java.nio.file.StandardOpenOption.READ;

/**
 * Represents a file system for persisting logs. Stores files in a local file system.
 */
public final class LocalFsLogFileSystem extends AbstractLogFileSystem {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final int DEFAULT_READ_BLOCK_SIZE = 256 * 1024;

	private final ExecutorService executorService;
	private final Path dir;

	private int readBlockSize = DEFAULT_READ_BLOCK_SIZE;

	/**
	 * Constructs a log file system, that runs in the given event loop, runs blocking IO operations in the specified executor,
	 * stores logs in the given directory.
	 *
	 * @param executorService executor for blocking IO operations
	 * @param dir             directory for storing log files
	 */
	private LocalFsLogFileSystem(ExecutorService executorService,
	                             Path dir) {
		this.executorService = executorService;
		this.dir = dir;
	}

	public static LocalFsLogFileSystem create(ExecutorService executorService, Path dir) {
		return new LocalFsLogFileSystem(executorService, dir);
	}

	public static LocalFsLogFileSystem create(ExecutorService executorService,
	                                          Path dir, String logName) {
		return create(executorService, dir.resolve(logName));
	}

	public LocalFsLogFileSystem withReadBlockSize(int readBlockSize) {
		this.readBlockSize = readBlockSize;
		return this;
	}

	public LocalFsLogFileSystem withReadBlockSize(MemSize readBlockSize) {
		return withReadBlockSize((int) readBlockSize.get());
	}

	private Path path(String logPartition, LogFile logFile) {
		return dir.resolve(fileName(logPartition, logFile));
	}

	@Override
	public CompletionStage<List<LogFile>> list(String logPartition) {
		Eventloop eventloop = getCurrentEventloop();
		return eventloop.callConcurrently(executorService, () -> {
			final List<LogFile> entries = new ArrayList<>();

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
			return entries;
		});
	}

	@Override
	public CompletionStage<StreamProducerWithResult<ByteBuf, Void>> read(String logPartition, LogFile logFile, long startPosition) {
		Eventloop eventloop = getCurrentEventloop();
		return AsyncFile.openAsync(eventloop, executorService, path(logPartition, logFile), new OpenOption[]{READ})
				.thenApply(file -> StreamProducerWithResult.wrap(
						StreamFileReader.readFileFrom(eventloop, file, readBlockSize, startPosition)));
	}

	@Override
	public CompletionStage<StreamConsumerWithResult<ByteBuf, Void>> write(String logPartition, LogFile logFile) {
		Eventloop eventloop = getCurrentEventloop();
		return AsyncFile.openAsync(eventloop, executorService, path(logPartition, logFile), StreamFileWriter.CREATE_OPTIONS).thenApply(file -> {
			StreamFileWriter writer = StreamFileWriter.create(eventloop, file, true);
			return StreamConsumerWithResult.create(writer, writer.getFlushStage());
		});
	}
}
