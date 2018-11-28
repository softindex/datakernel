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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.ChannelConsumerTransformer;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.csp.process.ChannelByteRanger;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.file.AsyncFile;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

/**
 * An implementation of {@link FsClient} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(LocalFsClient.class);

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Path storageDir;

	private MemSize readerBufferSize = MemSize.kilobytes(256);
	private boolean lazyOverrides = true;

	//region JMX
	private final PromiseStats writeBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats writeFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats readBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats readFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleMovePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleCopyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
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

	public LocalFsClient withLazyOverrides(boolean lazyOverrides) {
		this.lazyOverrides = lazyOverrides;
		return this;
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
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		checkNotNull(filename, "fileName");

		return ensureDirectory(filename)
				.thenCompose(path -> AsyncFile.openAsync(executor, path, new OpenOption[]{WRITE, offset == -1 ? CREATE_NEW : CREATE}, this))
				.thenCompose(file -> {
					logger.trace("writing to file: {}: {}", file, this);
					return file.size()
							.thenCompose(size -> {
								if (offset != -1) {
									if (size == null) {
										return Promise.ofException(FILE_NOT_FOUND);
									}
									if (offset > size) {
										return Promise.ofException(new StacklessException(LocalFsClient.class,
												"Trying to append to file " + filename + " at offset " + offset + ", which is greater than the file size " + size));
									}
								}
								return Promise.of(
										ChannelFileWriter.create(file)
												.withOffset(offset == -1 ? 0L : lazyOverrides ? size : offset)
												.withForceOnClose(true)
												.withAcknowledgement(ack ->
														ack.whenComplete(writeFinishPromise.recordStats()))
												.transformWith(lazyOverrides && offset != -1 && offset != size ?
														ChannelByteRanger.drop(size - offset) :
														ChannelConsumerTransformer.identity()));
							});
				})
				.whenComplete(toLogger(logger, TRACE, "upload", filename, this))
				.whenComplete(writeBeginPromise.recordStats());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		checkNotNull(filename, "fileName");
		checkArgument(offset >= 0, "Data offset must be greater than or equal to zero");
		checkArgument(length >= -1, "Data length must be either -1 or greater than or equal to zero");

		Path path = storageDir.resolve(filename).normalize();
		if (!path.startsWith(storageDir)) {
			return Promise.ofException(new RemoteFsException(LocalFsClient.class, "File " + filename + " goes outside of the root directory"));
		}

		return AsyncFile.size(executor, path)
				.thenCompose(size -> {
					if (size == null) {
						return Promise.ofException(FILE_NOT_FOUND);
					}
					String repr = filename + "(size=" + size + (offset != 0 ? ", offset=" + offset : "") + (length != -1 ? ", length=" + length : "") + ")";
					if (offset > size) {
						return Promise.ofException(new StacklessException(LocalFsClient.class, "Offset exceeds file size for " + repr));
					}
					if (length != -1 && offset + length > size) {
						return Promise.ofException(new StacklessException(LocalFsClient.class, "Boundaries size exceed file for " + repr));
					}
					return AsyncFile.openAsync(executor, path, ChannelFileReader.READ_OPTIONS, this)
							.thenApply(file -> {
								logger.trace("reading from file {}: {}", repr, this);
								return ChannelFileReader.readFile(file)
										.withBufferSize(readerBufferSize)
										.withOffset(offset)
										.withLength(length == -1 ? Long.MAX_VALUE : length)
										.withEndOfStream(eos ->
												eos.whenComplete(readFinishPromise.recordStats()));
							});
				})
				.whenComplete(toLogger(logger, TRACE, "download", filename, offset, length, this))
				.whenComplete(readBeginPromise.recordStats());
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		return Promises.all(
				changes.entrySet()
						.stream()
						.map(entry ->
								move(entry.getKey(), entry.getValue())
										.whenException(e -> logger.warn("Failed to move file {} into {}: {}", entry.getKey(), entry.getValue(), e))
										.thenApplyEx(($, e) -> e != null ? null : entry.getKey())))
				.whenComplete(toLogger(logger, TRACE, "move", changes, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> move(String filename, String targetName) {
		return Promise.ofRunnable(executor,
				() -> {
					synchronized (this) {
						try {
							Path filePath = resolveFilePath(filename);
							Path targetPath = resolveFilePath(targetName);

							if (!Files.isDirectory(filePath)) {
								long fileSize = Files.isRegularFile(filePath) ? Files.size(filePath) : -1;
								long targetSize = Files.isRegularFile(targetPath) ? Files.size(targetPath) : -1;

								// moving 'nothing' into 'nothing', this is a noop
								if (fileSize == -1 && targetSize == -1) {
									return;
								}

								// assuming it did move in a possible previous erroneous attempt
								if (targetSize >= fileSize) {
									if (fileSize != -1) { // if original still exists, delete it
										Files.delete(filePath);
									}
									return;
								}
							} else if (Files.exists(targetPath)) {
								throw new StacklessException(LocalFsClient.class, "Trying to move directory " + filename + " into existing file " + targetName);
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
						} catch (IOException | StacklessException e) {
							throw new UncheckedException(e);
						}
					}
				})
				.whenComplete(toLogger(logger, TRACE, "move", filename, targetName, this))
				.whenComplete(singleMovePromise.recordStats());
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		return Promises.all(
				changes.entrySet()
						.stream()
						.map(entry ->
								copy(entry.getKey(), entry.getValue())
										.whenException(e -> logger.warn("Failed to copy file {} into {}: {}", entry.getKey(), entry.getValue(), e))
										.thenApplyEx(($, e) -> e != null ? null : entry.getKey())))
				.whenComplete(toLogger(logger, TRACE, "copy", changes, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copy(String filename, String copyName) {
		return Promise.ofRunnable(executor,
				() -> {
					synchronized (this) {
						try {
							Path filePath = resolveFilePath(filename);
							Path copyPath = resolveFilePath(copyName);

							// copying 'nothing' into target equals deleting the target
							if (!Files.isRegularFile(filePath)) {
								Files.deleteIfExists(copyPath);
								return;
							}
							// copying anything into existing file replaces that file with the thing that we copied
							if (Files.isRegularFile(copyPath)) {
								Files.deleteIfExists(copyPath);
							}

							// not using ensureDirectory so we have only one executor task
							Files.createDirectories(copyPath.getParent());
							try {
								// try to create a hardlink
								Files.createLink(copyPath, filePath);
							} catch (UnsupportedOperationException | SecurityException e) {
								// if couldnt, then just actually copy it
								Files.copy(filePath, copyPath, REPLACE_EXISTING);
							}
						} catch (IOException e) {
							throw new UncheckedException(e);
						}
					}
				})
				.whenComplete(toLogger(logger, TRACE, "copy", filename, copyName, this))
				.whenComplete(singleCopyPromise.recordStats());
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		return Promise.ofRunnable(executor,
				() -> {
					synchronized (this) {
						try {
							walkFiles(glob, (meta, path) -> {
								logger.trace("deleting file: {}: {}", meta, this);
								Files.delete(path);
							});
						} catch (IOException e) {
							throw new UncheckedException(e);
						}
					}
				})
				.whenComplete(toLogger(logger, TRACE, "delete", glob, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return Promise.ofCallable(executor,
				() -> {
					List<FileMetadata> list = new ArrayList<>();
					walkFiles(glob, (meta, $) -> list.add(meta));
					return list;
				})
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return Promise.of(null); // local fs is always awailable
	}

	@Override
	public Promise<FileMetadata> getMetadata(String filename) {
		return Promise.ofCallable(executor,
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
	public Promise<Void> start() {
		return AsyncFile.createDirectories(executor, storageDir, null);
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
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

	private Promise<Path> ensureDirectory(String filePath) {
		return Promise.ofCallable(executor,
				() -> {
					Path path = resolveFilePath(filePath);
					Files.createDirectories(path.getParent());
					return path;
				})
				.whenComplete(toLogger(logger, TRACE, "ensureDirectory", filePath, this));
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
	public PromiseStats getWriteBeginPromise() {
		return writeBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getWriteFinishPromise() {
		return writeFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getReadBeginPromise() {
		return readBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getReadFinishPromise() {
		return readFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getSingleMovePromise() {
		return singleMovePromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getSingleCopyPromise() {
		return singleCopyPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}
	//endregion
}
