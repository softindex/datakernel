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

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.stats.StreamRegistry;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;
import static java.nio.file.StandardOpenOption.READ;

/**
 * Represents a file system for persisting logs. Stores files in a local file system.
 */
public final class LocalFsLogFileSystem extends AbstractLogFileSystem implements EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final MemSize DEFAULT_READ_BLOCK_SIZE = MemSize.kilobytes(256);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final Path dir;

	private MemSize readBlockSize = DEFAULT_READ_BLOCK_SIZE;

	private final StageStats stageList = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageRead = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageWrite = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final StreamRegistry<String> streamReads = StreamRegistry.create();
	private final StreamRegistry<String> streamWrites = StreamRegistry.create();

	private final StreamStatsDetailed<ByteBuf> streamReadStats = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> streamWriteStats = StreamStats.detailed(forByteBufs());

	/**
	 * Constructs a log file system, that runs in the given event loop, runs blocking IO operations in the specified executor,
	 * stores logs in the given directory.
	 *
	 * @param eventloop
	 * @param executorService executor for blocking IO operations
	 * @param dir             directory for storing log files
	 */
	private LocalFsLogFileSystem(Eventloop eventloop, ExecutorService executorService, Path dir) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.dir = dir;
	}

	public static LocalFsLogFileSystem create(Eventloop eventloop, ExecutorService executorService, Path dir) {
		return new LocalFsLogFileSystem(eventloop, executorService, dir);
	}

	public static LocalFsLogFileSystem create(Eventloop eventloop, ExecutorService executorService, Path dir, String logName) {
		return create(eventloop, executorService, dir.resolve(logName));
	}

	public LocalFsLogFileSystem withReadBlockSize(MemSize readBlockSize) {
		this.readBlockSize = readBlockSize;
		return this;
	}

	private Path path(String logPartition, LogFile logFile) {
		return dir.resolve(fileName(logPartition, logFile));
	}

	@Override
	public Stage<List<LogFile>> list(String logPartition) {
		Eventloop eventloop = getCurrentEventloop();
		return Stage.ofCallable(executorService, () -> {
			List<LogFile> entries = new ArrayList<>();

			Files.createDirectories(dir);
			Files.walkFileTree(dir, new FileVisitor<Path>() {
				@Override
				public FileVisitResult preVisitDirectory(Path dir1, BasicFileAttributes attrs) throws IOException {
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
				public FileVisitResult postVisitDirectory(Path dir1, IOException exc) throws IOException {
					if (exc != null) {
						logger.error("postVisitDirectory error", exc);
					}
					return FileVisitResult.CONTINUE;
				}
			});
			return entries;
		}).whenComplete(stageList.recordStats());
	}

	@Override
	public Stage<StreamProducerWithResult<ByteBuf, Void>> read(String logPartition, LogFile logFile, long startPosition) {
		return AsyncFile.openAsync(executorService, path(logPartition, logFile), new OpenOption[]{READ})
				.whenComplete(stageRead.recordStats())
				.thenApply(file -> StreamFileReader.readFile(file).withBufferSize(readBlockSize).withStartingPosition(startPosition)
						.with(streamReads.newEntry(logPartition + ":" + logFile + "@" + startPosition))
						.with(streamReadStats)
						.withEndOfStreamAsResult()
						.withLateBinding());
	}

	@Override
	public Stage<StreamConsumerWithResult<ByteBuf, Void>> write(String logPartition, LogFile logFile) {
		return AsyncFile.openAsync(executorService, path(logPartition, logFile), StreamFileWriter.CREATE_OPTIONS)
				.whenComplete(stageWrite.recordStats())
				.thenApply(file -> StreamFileWriter.create(file).withForceOnClose(true).withFlushAsResult()
						.with(streamWrites.newEntry(logPartition + ":" + logFile))
						.with(streamWriteStats)
						.withLateBinding());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public String getDir() {
		return dir.toString();
	}

	@JmxAttribute
	public int getReadBlockSize() {
		return readBlockSize.toInt();
	}

	@JmxAttribute
	public void setReadBlockSize(int readBlockSize) {
		this.readBlockSize = MemSize.of(readBlockSize);
	}

	@JmxAttribute
	public StageStats getStageList() {
		return stageList;
	}

	@JmxAttribute
	public StageStats getStageRead() {
		return stageRead;
	}

	@JmxAttribute
	public StageStats getStageWrite() {
		return stageWrite;
	}

	@JmxAttribute
	public StreamRegistry getStreamReads() {
		return streamReads;
	}

	@JmxAttribute
	public StreamRegistry getStreamWrites() {
		return streamWrites;
	}

	@JmxAttribute
	public StreamStatsDetailed getStreamReadStats() {
		return streamReadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getStreamWriteStats() {
		return streamWriteStats;
	}
}
