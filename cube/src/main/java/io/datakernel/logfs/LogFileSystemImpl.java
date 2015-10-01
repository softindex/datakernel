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

import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.postExceptionConcurrently;
import static io.datakernel.async.AsyncCallbacks.postResultConcurrently;

/**
 * Represents a file system for persisting logs. Stores files in a local file system.
 */
public final class LogFileSystemImpl implements LogFileSystem {
	private static final Logger logger = LoggerFactory.getLogger(LogFileSystemImpl.class);

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final Path dir;

	/**
	 * Constructs a log file system, that runs in the given event loop, runs blocking IO operations in the specified executor,
	 * stores logs in the given directory.
	 *
	 * @param eventloop       event loop, which log file system is to run
	 * @param executorService executor for blocking IO operations
	 * @param dir             directory for storing log files
	 */
	public LogFileSystemImpl(Eventloop eventloop, ExecutorService executorService,
	                         Path dir) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.dir = dir;
	}

	public LogFileSystemImpl(Eventloop eventloop, ExecutorService executorService, Path dir, String logName) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.dir = dir.resolve(logName);
	}

	private static final class PartitionAndFile {
		private final String logPartition;
		private final LogFile logFile;

		private PartitionAndFile(String logPartition, LogFile logFile) {
			this.logPartition = logPartition;
			this.logFile = logFile;
		}
	}

	private static PartitionAndFile parse(Path path) {
		String s = path.getFileName().toString();
		int index1 = s.indexOf('.');
		if (index1 == -1)
			return null;
		String name = s.substring(0, index1);
		if (name.isEmpty())
			return null;
		s = s.substring(index1 + 1);
		if (!s.endsWith(".log"))
			return null;
		s = s.substring(0, s.length() - 4);
		int n = 0;
		int index2 = s.indexOf('-');
		String logPartition;
		if (index2 != -1) {
			logPartition = s.substring(0, index2);
			try {
				n = Integer.parseInt(s.substring(index2 + 1));
			} catch (NumberFormatException e) {
				return null;
			}
		} else {
			logPartition = s;
		}
		if (logPartition.isEmpty())
			return null;
		return new PartitionAndFile(logPartition, new LogFile(name, n));
	}

	private Path path(String logPartition, LogFile logFile) {
		String filename = logFile.getName() + "." + logPartition + (logFile.getN() != 0 ? "-" + logFile.getN() : "") + ".log";
		return dir.resolve(filename);
	}

	@Override
	public void makeUniqueLogFile(String logPartition, final String logName, final ResultCallback<LogFile> callback) {
		list(logPartition, new ForwardingResultCallback<List<LogFile>>(callback) {
			@Override
			public void onResult(List<LogFile> logFiles) {
				int chunkN = 0;
				for (LogFile logFile : logFiles) {
					if (logFile.getName().equals(logName)) {
						chunkN = Math.max(chunkN, logFile.getN() + 1);
					}
				}
				callback.onResult(new LogFile(logName, chunkN));
			}
		});
	}

	@Override
	public void list(final String logPartition, final ResultCallback<List<LogFile>> callback) {
		final Eventloop.ConcurrentOperationTracker concurrentOperationTracker = eventloop.startConcurrentOperation();
		executorService.execute(new Runnable() {
			@Override
			public void run() {
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
							PartitionAndFile partitionAndFile = parse(file);
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
					postResultConcurrently(eventloop, callback, entries);
				} catch (IOException e) {
					// TODO ?
					logger.error("walkFileTree error", e);
					postExceptionConcurrently(eventloop, callback, e);
				}

				concurrentOperationTracker.complete();
			}
		});
	}

	@Override
	public StreamFileReader reader(String logPartition, LogFile logFile, long positionFrom) {
		return StreamFileReader.readFileFrom(eventloop, executorService, 1024 * 1024, path(logPartition, logFile), positionFrom);
	}

	@Override
	public StreamFileWriter writer(String logPartition, LogFile logFile) {
		return StreamFileWriter.createFile(eventloop, executorService, path(logPartition, logFile));
	}

}
