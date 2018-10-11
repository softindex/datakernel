/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.remotefs;

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.UncheckedException;
import io.datakernel.file.AsyncFile;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialConsumerFunction;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.file.SerialFileReader;
import io.datakernel.serial.file.SerialFileWriter;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.file.FileUtils.isWildcard;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;
import static java.util.stream.Collectors.toSet;

/**
 * An implementation of {@link FsClient} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(LocalFsClient.class);

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Path storageDir;

	private MemSize readerBufferSize = MemSize.kilobytes(256);

	//region JMX
	private final StageStats writeBeginStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats writeFinishStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats readBeginStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats readFinishStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats moveStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats singleMoveStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats copyStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats singleCopyStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats listStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats deleteStage = StageStats.create(Duration.ofMinutes(5));
	//endregion

	// region creators
	private LocalFsClient(Eventloop eventloop, ExecutorService executor, Path storageDir) {
		this.eventloop = checkNotNull(eventloop, "eventloop");
		this.executor = checkNotNull(executor, "executor");
		this.storageDir = checkNotNull(storageDir, "storageDir").normalize();

		try {
			Files.createDirectories(storageDir);
		} catch (IOException e) {
			throw new AssertionError("Failed creating storage directory", e);
		}
	}

	public static LocalFsClient create(Eventloop eventloop, ExecutorService executor, Path storageDir) {
		return new LocalFsClient(eventloop, executor, storageDir);
	}

	/**
	 * Sets the buffer size for reading files from the filesystem.
	 */
	public LocalFsClient withReaderBufferSize(MemSize size) {
		readerBufferSize = size;
		return this;
	}
	// endregion

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		checkNotNull(filename, "fileName");

		return ensureDirectory(filename)
				.thenCompose(path -> AsyncFile.openAsync(executor, path, new OpenOption[]{WRITE, offset == -1 ? CREATE_NEW : CREATE}, this))
				.thenCompose(file -> {
					logger.trace("writing to file: {}: {}", file, this);
					return file.size()
							.thenCompose(size -> {
								if (offset != -1) {
									if (size == null) {
										return Stage.ofException(new RemoteFsException(LocalFsClient.class, "Trying to append to non-existent file"));
									}
									if (offset > size) {
										return Stage.ofException(new RemoteFsException(LocalFsClient.class, "Trying to append at offset greater than the file size"));
									}
								}
								return Stage.of(
										SerialFileWriter.create(file)
												.withOffset(offset == -1 ? 0L : size)
												.withForceOnClose(true)
												.withAcknowledgement(ack ->
														ack.whenComplete(writeFinishStage.recordStats()))
												.apply(offset != -1 && offset != size ?
														SerialByteBufCutter.create(size - offset) :
														SerialConsumerFunction.identity()));
							});
				})
				.whenComplete(toLogger(logger, TRACE, "upload", filename, this))
				.whenComplete(writeBeginStage.recordStats());
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		checkNotNull(filename, "fileName");
		checkArgument(offset >= 0, "Data offset must be greater than or equal to zero");
		checkArgument(length >= -1, "Data length must be either -1 or greater than or equal to zero");

		Path path = storageDir.resolve(filename).normalize();
		if (!path.startsWith(storageDir)) {
			return Stage.ofException(new RemoteFsException(LocalFsClient.class, "File " + filename + " goes outside of the root directory"));
		}

		return AsyncFile.size(executor, path)
				.thenCompose(size -> {
					if (size == null) {
						return Stage.ofException(new RemoteFsException(LocalFsClient.class, "File not found: " + filename));
					}
					String repr = filename + "(size=" + size + (offset != 0 ? ", offset=" + offset : "") + (length != -1 ? ", length=" + length : "");
					if (offset > size) {
						return Stage.ofException(new RemoteFsException(LocalFsClient.class, "Offset exceeds file size for " + repr));
					}
					if (length != -1 && offset + length > size) {
						return Stage.ofException(new RemoteFsException(LocalFsClient.class, "Boundaries exceed file for " + repr));
					}
					return AsyncFile.openAsync(executor, path, SerialFileReader.READ_OPTIONS, this)
							.thenApply(file -> {
								logger.trace("reading from file {}: {}", repr, this);
								return SerialFileReader.readFile(file)
										.withBufferSize(readerBufferSize)
										.withOffset(offset)
										.withLength(length == -1 ? Long.MAX_VALUE : length)
										.withEndOfStream(eos ->
												eos.whenComplete(readFinishStage.recordStats()));
							});
				})
				.whenComplete(toLogger(logger, TRACE, "download", filename, offset, length, this))
				.whenComplete(readBeginStage.recordStats());
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return Stages.toList(changes.entrySet().stream().map(e ->
				move(e.getKey(), e.getValue())
						.whenException(err -> logger.warn("Failed to move file {} into {}: {}", e.getKey(), e.getValue(), err))
						.thenApplyEx(($, err) -> err != null ? null : e.getKey())))
				.thenApply(res -> res.stream().filter(Objects::nonNull).collect(toSet()))
				.whenComplete(toLogger(logger, TRACE, "move", changes, this))
				.whenComplete(moveStage.recordStats());
	}

	@Override
	public Stage<Void> move(String filename, String targetName) {
		return Stage.ofRunnable(executor,
				() -> {
					synchronized (this) {
						try {
							Path filePath = resolveFilePath(filename);
							Path targetPath = resolveFilePath(targetName);

							if (Files.isDirectory(filePath)) {
								if (Files.exists(targetPath)) {
									throw new RemoteFsException(LocalFsClient.class, "Trying to move directory " + filename + " into existing file " + targetName);
								}
							} else {
								long fileSize = Files.isRegularFile(filePath) ? Files.size(filePath) : -1;
								long targetSize = Files.isRegularFile(targetPath) ? Files.size(targetPath) : -1;

								if (fileSize == -1 && targetSize == -1) {
									throw new RemoteFsException(LocalFsClient.class, "No file " + filename + ", neither file " + targetName + " were found");
								}

								// assuming it did move in a possible previous erroneous attempt
								if (targetSize >= fileSize) {
									if (fileSize != -1) { // if original still exists, delete it
										Files.delete(filePath);
									}
									return;
								}
							}

							// explicitly set timestamp to eventloop time source
							Files.setLastModifiedTime(filePath, FileTime.fromMillis(eventloop.currentTimeMillis()));
							// not using ensureDirectory so we have only one executor task
							Files.createDirectories(targetPath.getParent());
							try {
								Files.move(filePath, targetPath, REPLACE_EXISTING, ATOMIC_MOVE);
							} catch (AtomicMoveNotSupportedException e) {
								logger.warn("Atomic move were not supported when moving {} into {}", filename, targetName, e);
								Files.move(filePath, targetPath, REPLACE_EXISTING);
							}
						} catch (IOException | RemoteFsException e) {
							throw new UncheckedException(e);
						}
					}
				})
				.whenComplete(toLogger(logger, TRACE, "move", filename, targetName, this))
				.whenComplete(singleMoveStage.recordStats());
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return Stages.toList(changes.entrySet().stream().map(e ->
				copy(e.getKey(), e.getValue())
						.whenException(err -> logger.warn("Failed to copy file {} into {}: {}", e.getKey(), e.getValue(), err))
						.thenApplyEx(($, err) -> err != null ? null : e.getKey())))
				.thenApply(res -> res.stream().filter(Objects::nonNull).collect(toSet()))
				.whenComplete(toLogger(logger, TRACE, "copy", changes, this))
				.whenComplete(copyStage.recordStats());
	}

	@Override
	public Stage<Void> copy(String filename, String copyName) {
		return Stage.ofRunnable(executor,
				() -> {
					synchronized (this) {
						try {
							Path filePath = resolveFilePath(filename);
							Path copyPath = resolveFilePath(copyName);

							if (!Files.isRegularFile(filePath)) {
								throw new RemoteFsException(LocalFsClient.class, "No file " + filename + " were found");
							}
							if (Files.isRegularFile(copyPath)) {
								throw new RemoteFsException(LocalFsClient.class, "File " + copyName + " already exists!");
							}

							// not using ensureDirectory so we have only one executor task
							Files.createDirectories(copyPath.getParent());
							try {
								// try to create a hardlink
								Files.createLink(copyPath, filePath);
							} catch (UnsupportedOperationException | SecurityException e) {
								// if couldnt, then just actually copy it
								Files.copy(filePath, copyPath);
							}
						} catch (IOException | RemoteFsException e) {
							throw new UncheckedException(e);
						}
					}
				})
				.whenComplete(toLogger(logger, TRACE, "copy", filename, copyName, this))
				.whenComplete(singleCopyStage.recordStats());
	}

	@Override
	public Stage<Void> delete(String glob) {
		return Stage.ofRunnable(executor,
				() -> {
					synchronized (this) {
						try {
							walkFiles(glob, (meta, path) -> {
								logger.trace("deleting file: {}: {}", meta, this);
								Files.delete(path);
							});
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				})
				.whenComplete(toLogger(logger, TRACE, "delete", glob, this))
				.whenComplete(deleteStage.recordStats());
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return Stage.ofCallable(executor,
				() -> {
					List<FileMetadata> list = new ArrayList<>();
					walkFiles(glob, (meta, $) -> list.add(meta));
					return list;
				})
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listStage.recordStats());
	}

	@Override
	public Stage<Void> ping() {
		return Stage.of(null); // local fs is always awailable
	}

	@Override
	public Stage<FileMetadata> getMetadata(String filename) {
		return Stage.ofCallable(executor,
				() -> {
					Path file = storageDir.resolve(filename);
					if (Files.isRegularFile(file)) {
						return getFileMeta(file);
					}
					return null;
				});
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Stage<Void> start() {
		return AsyncFile.createDirectories(executor, storageDir, null);
	}

	@Override
	public Stage<Void> stop() {
		return Stage.complete();
	}

	@Override
	public FsClient subfolder(String folder) {
		return new LocalFsClient(eventloop, executor, storageDir.resolve(folder));
	}

	@Override
	public String toString() {
		return "LocalFsClient{storageDir=" + storageDir + '}';
	}

	private Path resolveFilePath(String filePath) throws IOException {
		Path path = storageDir.resolve(filePath).normalize();
		if (!path.startsWith(storageDir)) {
			throw new IOException("File " + filePath + " goes outside of the storage directory");
		}
		return path;
	}

	private Stage<Path> ensureDirectory(String filePath) {
		return Stage.ofCallable(executor,
				() -> {
					Path path = resolveFilePath(filePath);
					Files.createDirectories(path.getParent());
					return path;
				}).whenComplete(toLogger(logger, TRACE, "ensureDirectory", filePath, this));
	}

	private void walkFiles(String glob, Walker walker) throws IOException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return;
		}
		// optimization for listing all files
		if ("**".equals(glob)) {
			Files.walkFileTree(storageDir, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (Files.isRegularFile(file)) {
						walker.accept(getFileMeta(file), file);
					}
					return CONTINUE;
				}
			});
			return;
		}
		// optimization for single-file requests
		if (!isWildcard(glob)) {
			Path file = storageDir.resolve(glob);
			if (Files.isRegularFile(file)) {
				walker.accept(getFileMeta(file), file);
			}
			return;
		}
		PathMatcher matcher = storageDir.getFileSystem().getPathMatcher("glob:" + glob);
		Files.walkFileTree(storageDir, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (matcher.matches(storageDir.relativize(file))) {
					if (Files.isRegularFile(file)) {
						walker.accept(getFileMeta(file), file);
					}
				}
				return CONTINUE;
			}
		});
	}

	private FileMetadata getFileMeta(Path file) throws IOException {
		return new FileMetadata(storageDir.relativize(file).toString(), Files.size(file), Files.getLastModifiedTime(file).toMillis());
	}

	@FunctionalInterface
	interface Walker {

		void accept(FileMetadata meta, Path path) throws IOException;
	}

	//region JMX
	@JmxAttribute
	public StageStats getWriteBeginStage() {
		return writeBeginStage;
	}

	@JmxAttribute
	public StageStats getWriteFinishStage() {
		return writeFinishStage;
	}

	@JmxAttribute
	public StageStats getReadBeginStage() {
		return readBeginStage;
	}

	@JmxAttribute
	public StageStats getReadFinishStage() {
		return readFinishStage;
	}

	@JmxAttribute
	public StageStats getMoveStage() {
		return moveStage;
	}

	@JmxAttribute
	public StageStats getSingleMoveStage() {
		return singleMoveStage;
	}

	@JmxAttribute
	public StageStats getCopyStage() {
		return copyStage;
	}

	@JmxAttribute
	public StageStats getSingleCopyStage() {
		return singleCopyStage;
	}

	@JmxAttribute
	public StageStats getListStage() {
		return listStage;
	}

	@JmxAttribute
	public StageStats getDeleteStage() {
		return deleteStage;
	}
	//endregion
}
