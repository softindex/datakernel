/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.csp.process.ChannelByteRanger;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.datakernel.file.AsyncFile;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.MemSize;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.async.Promise.ofBlockingCallable;
import static io.datakernel.remotefs.RemoteFsUtils.*;
import static io.datakernel.util.CollectionUtils.set;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.Files.newByteChannel;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Collections.emptyList;

/**
 * An implementation of {@link FsClient} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(LocalFsClient.class);

	private static final String TOMBSTONE_ATTRIBUTE = "tombstone";
	private static final String REVISION_ATTRIBUTE = "revision";

	private final Eventloop eventloop;
	private final Path storage;
	private final Executor executor;
	private final Object lock;

	private MemSize readerBufferSize = MemSize.kilobytes(256);
	private boolean lazyOverrides = true;
	@Nullable
	private Long defaultRevision = DEFAULT_REVISION;

	CurrentTimeProvider now;

	//region JMX
	private final PromiseStats writeBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats writeFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats readBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats readFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleMovePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleCopyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleDeletePromise = PromiseStats.create(Duration.ofMinutes(5));
	//endregion

	// region creators
	private LocalFsClient(Eventloop eventloop, Path storage, Executor executor, @Nullable Object lock) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;
		this.lock = lock != null ? lock : this;

		now = eventloop;

		try {
			Files.createDirectories(storage);
		} catch (IOException e) {
			throw new AssertionError("Failed creating storage directory", e);
		}
	}

	public static LocalFsClient create(Eventloop eventloop, Path storageDir) {
		return new LocalFsClient(eventloop, storageDir, Executors.newSingleThreadExecutor(), null);
	}

	/**
	 * Use this to synchronize multiple LocalFsClient's over some filesystem space
	 * that they may all try to access (same storage folder, one storage is subfolder of another etc.)
	 */
	public static LocalFsClient create(Eventloop eventloop, Path storageDir, Object lock) {
		return new LocalFsClient(eventloop, storageDir, Executors.newSingleThreadExecutor(), lock);
	}

	public static LocalFsClient create(Eventloop eventloop, Path storageDir, Executor executor) {
		return new LocalFsClient(eventloop, storageDir, executor, null);
	}

	public static LocalFsClient create(Eventloop eventloop, Path storageDir, Executor executor, Object lock) {
		return new LocalFsClient(eventloop, storageDir, executor, lock);
	}

	public LocalFsClient withLazyOverrides(boolean lazyOverrides) {
		this.lazyOverrides = lazyOverrides;
		return this;
	}

	public LocalFsClient withDefaultRevision(long defaultRevision) {
		this.defaultRevision = defaultRevision;
		return this;
	}

	public LocalFsClient withRevisions() {
		this.defaultRevision = null;
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
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long offset, long revision) {
		checkNotNull(name, "name");
		checkArgument(offset >= 0, "offset < 0");

		return ofBlockingCallable(executor,
				() -> {
					synchronized (lock) {
						Path path = resolve(name);
						AsyncFile file;
						long skip;
						Long oldRev;

						if (defaultRevision != null) {
							if (revision != defaultRevision) {
								throw UNSUPPORTED_REVISION;
							}
							// old logic with lazy overrides
							boolean exists = Files.isRegularFile(path);
							long size = exists ? Files.size(path) : 0;
							if (offset > size) {
								throw OFFSET_TOO_BIG;
							}
							if (!exists) {
								Files.createDirectories(path.getParent());
							}
							file = AsyncFile.open(executor, path, set(CREATE, WRITE), this);
							skip = lazyOverrides ? size - offset : 0;
						} else if (Files.isRegularFile(path) && (oldRev = getLongAttribute(path, REVISION_ATTRIBUTE)) != null) {
							long size = Files.size(path);

							if (revision < oldRev) {         // existing file has higher revision, skip
								logger.info("tried to upload file {} (rev {}), but file {} (rev {}) already exists", name, revision, name, oldRev);

								return ChannelConsumers.<ByteBuf>recycling(); // TODO anton: handle recyclers over the net
							} else if (revision == oldRev) { // same revision, append-only and lazy overrides
								if (isTombstone(path)) { // tombstone with same revision wins, so behave like in revision < oldRev branch here
									return ChannelConsumers.<ByteBuf>recycling();
								}
								if (offset > size) {
									throw OFFSET_TOO_BIG;
								}
								logger.trace("appending to file: {}: {}", name, this);
								file = AsyncFile.open(executor, path, set(CREATE, WRITE), this);
								skip = lazyOverrides ? size - offset : 0;
							} else {                         // higher revision, no appends but override from scratch
								if (offset != 0) {
									throw OFFSET_TOO_BIG;
								}
								logger.trace("overriding file: {}: {}", name, this);
								file = AsyncFile.open(executor, path, set(CREATE, WRITE, TRUNCATE_EXISTING), this);
								skip = 0;
								removeAttribute(path, TOMBSTONE_ATTRIBUTE);
								setRevision(path, revision);
							}
						} else {
							if (offset != 0) {
								throw OFFSET_TOO_BIG;
							}
							logger.trace("new file: {}: {}", name, this);
							Files.createDirectories(path.getParent());
							file = AsyncFile.open(executor, path, set(CREATE_NEW, WRITE), this);
							skip = 0;
							removeAttribute(path, TOMBSTONE_ATTRIBUTE);
							setRevision(path, revision);
						}
						return ChannelFileWriter.create(file)
								.withForceOnClose(true)
								.withOffset(offset + skip)
								.transformWith(ChannelByteRanger.drop(skip));
					}
				})
				.map(consumer -> consumer
						// call withAcknowledgement in eventloop thread
						.withAcknowledgement(ack -> ack
								.whenComplete(writeFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "writing to file", name, offset, revision, this))))
				.whenComplete(writeBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "upload", name, offset, revision, this));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long length) {
		checkNotNull(name, "name");
		checkArgument(offset >= 0, "offset < 0");
		checkArgument(length >= -1, "length < -1");

		return ofBlockingCallable(executor,
				() -> {
					synchronized (lock) {
						Path path = resolve(name);
						if (!Files.isRegularFile(path)) {
							throw FILE_NOT_FOUND;
						}
						checkRange(Files.size(path), offset, length);
						return ChannelFileReader.readFile(AsyncFile.open(executor, path, ChannelFileReader.READ_OPTIONS, this))
								.withBufferSize(readerBufferSize)
								.withOffset(offset)
								.withLength(length == -1 ? Long.MAX_VALUE : length);
					}
				})
				.map(consumer -> consumer
						// call withAcknowledgement in eventloop thread
						.withEndOfStream(eos -> eos.whenComplete(readFinishPromise.recordStats())))
				.whenComplete(toLogger(logger, TRACE, "download", name, offset, length, this))
				.whenComplete(readBeginPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(String glob) {
		return ofBlockingCallable(executor, () -> doList(glob, true))
				.whenComplete(toLogger(logger, TRACE, "listEntities", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return ofBlockingCallable(executor, () -> doList(glob, false))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<Void> move(String name, String target, long targetRevision, long removeRevision) {
		return ofBlockingCallable(executor,
				() -> {
					synchronized (lock) {
						doMove(resolve(name), resolve(target), targetRevision, removeRevision);
					}
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "move", name, target, this))
				.whenComplete(singleMovePromise.recordStats());
	}

	@Override
	public Promise<Void> moveDir(String name, String target, long targetRevision, long removeRevision) {
		if (defaultRevision == null) {
			return FsClient.super.moveDir(name, target, targetRevision, removeRevision);
		} else if (targetRevision != defaultRevision || removeRevision != defaultRevision) {
			return Promise.ofException(UNSUPPORTED_REVISION);
		}
		return ofBlockingCallable(executor,
				() -> {
					synchronized (lock) {
						Path path = resolve(name);
						Path targetPath = resolve(target);
						// cannot move directory into existing file or directory
						if (Files.exists(targetPath)) {
							throw FILE_EXISTS;
						}
						// do nothing if it is not a directory, "it did not match"
						if (Files.isDirectory(path)) {
							Files.move(path, targetPath, ATOMIC_MOVE);
						}
					}
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "move", name, target, this))
				.whenComplete(singleMovePromise.recordStats());
	}

	@Override
	public Promise<Void> copy(String name, String target, long targetRevision) {
		return ofBlockingCallable(executor,
				() -> {
					synchronized (lock) {
						doMove(resolve(name), resolve(target), targetRevision, null);
					}
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "copy", name, target, this))
				.whenComplete(singleCopyPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(String name, long revision) {
		return ofBlockingCallable(executor,
				() -> {
					synchronized (lock) {
						doDelete(resolve(name), revision);
					}
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "delete", name, this))
				.whenComplete(singleDeletePromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete(); // local fs is always awailable
	}

	@Override
	public Promise<FileMetadata> getMetadata(String name) {
		return ofBlockingCallable(executor, () -> getFileMeta(resolve(name), true));
	}

	@Override
	public FsClient subfolder(String folder) {
		if (folder.length() == 0) {
			return this;
		}
		try {
			LocalFsClient client = new LocalFsClient(eventloop, resolve(folder), executor, lock);
			client.readerBufferSize = readerBufferSize;
			client.lazyOverrides = lazyOverrides;
			client.defaultRevision = defaultRevision;
			return client;
		} catch (StacklessException e) {
			// when folder points outside of the storage directory
			throw new IllegalArgumentException("illegal subfolder: " + folder, e);
		}
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return AsyncFile.createDirectories(executor, storage).materialize();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "LocalFsClient{storage=" + storage + '}';
	}

	private Path resolve(String name) throws StacklessException {
		Path path = storage.resolve(File.separatorChar == '\\' ? name.replace('/', '\\') : name).normalize();
		if (path.startsWith(storage)) {
			return path;
		}
		throw BAD_PATH;
	}

	private boolean isTombstone(Path path) throws IOException {
		Boolean attr = getBooleanAttribute(path, TOMBSTONE_ATTRIBUTE);
		return attr != null && attr;
	}

	private void setRevision(Path path, long revision) throws IOException {
		assert defaultRevision == null || revision != defaultRevision;
		setLongAttribute(path, REVISION_ATTRIBUTE, revision);
	}

	private void tryHardlinkOrCopy(Path path, Path targetPath) throws IOException {
		Files.deleteIfExists(targetPath);
		try {
			// try to create a hardlink
			Files.createLink(targetPath, path);
		} catch (UnsupportedOperationException | SecurityException e) {
			// if couldnt, then just actually copy it
			Files.copy(path, targetPath);
		}
	}

	private void doMove(Path path, Path targetPath, long targetRevision, @Nullable Long removeRevision) throws StacklessException, IOException {
		if (Files.isDirectory(path) || Files.isDirectory(targetPath)) {
			throw MOVING_DIRS;
		}
		// noop when paths are equal and also copying 'nothing' into anything is a noop
		if (path.equals(targetPath) || !Files.isRegularFile(path)) {
			return;
		}

		// check the revisions
		if (defaultRevision != null) {
			if (targetRevision != defaultRevision) {
				throw UNSUPPORTED_REVISION;
			}
			if (removeRevision != null && !removeRevision.equals(defaultRevision)) {
				throw UNSUPPORTED_REVISION;
			}
			// with old logic we cannot move into existing file
			if (Files.isRegularFile(targetPath)) {
				throw FILE_EXISTS;
			}
		} else if (Files.isRegularFile(targetPath)) {
			// copying anything into existing file with greater revision is a noop
			Long oldRev = getLongAttribute(targetPath, REVISION_ATTRIBUTE);
			if (oldRev != null && targetRevision <= oldRev) {
				if (logger.isTraceEnabled()) {
					logger.trace("ignoring attempt to replace file {} (rev {}) with file {} (rev {})",
							storage.relativize(path), oldRev, storage.relativize(targetPath), targetRevision);
				}
				if (removeRevision != null) {
					// cant replace target but try to remove the original when requested
					doDelete(path, removeRevision);
				}
				return;
			}
		}

		Files.createDirectories(targetPath.getParent());
		if (removeRevision != null) {
			// atomically move
			Files.move(path, targetPath, ATOMIC_MOVE);
			// due to above move nothing will be deleted, but a tombstone needs to be placed there
			doDelete(path, removeRevision);
		} else {
			tryHardlinkOrCopy(path, targetPath);
		}

		// and set target revision if revisions are enabled
		if (defaultRevision == null) {
			setRevision(targetPath, targetRevision);
		}
	}

	private void doDelete(Path path, long revision) throws IOException, StacklessException {
		if (defaultRevision != null) {
			if (revision != defaultRevision) {
				throw UNSUPPORTED_REVISION;
			}
			// old logic - just delete the file
			Files.deleteIfExists(path);
			return;
		}
		if (Files.exists(path)) {
			Long oldRev = getLongAttribute(path, REVISION_ATTRIBUTE);
			// ignoring existense of file if it has no revision attribute
			if (oldRev != null && revision < oldRev) {
				// file has greater revision, we do nothing
				if (logger.isTraceEnabled()) {
					logger.trace("ignoring attempt to replace file {} (rev {}) with a tombstone for it (rev {})",
							storage.relativize(path), oldRev, revision);
				}
				return;
			}
		} else {
			Files.createDirectories(path.getParent());
		}
		newByteChannel(path, EnumSet.of(CREATE, WRITE, TRUNCATE_EXISTING)).close();
		setBooleanAttribute(path, TOMBSTONE_ATTRIBUTE, true);
		setRevision(path, revision);
	}

	@Nullable
	private FileMetadata getFileMeta(Path path, boolean includeTombstone) throws IOException {
		synchronized (lock) {
			if (!Files.isRegularFile(path)) {
				return null;
			}
			Long revision = getLongAttribute(path, REVISION_ATTRIBUTE);
			if (defaultRevision == null && revision == null) {
				return null;
			}
			if (revision == null) {
				revision = defaultRevision;
			}
			String name = storage.relativize(path).toString();
			name = File.separatorChar == '\\' ? name.replace('\\', '/') : name;
			long timestamp = Files.getLastModifiedTime(path).toMillis();
			if (isTombstone(path)) {
				return includeTombstone ? FileMetadata.tombstone(name, timestamp, revision) : null;
			}
			return FileMetadata.of(name, Files.size(path), timestamp, revision);
		}
	}

	private List<FileMetadata> doList(String glob, boolean listTombstones) throws IOException, StacklessException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return emptyList();
		}
		List<FileMetadata> list = new ArrayList<>();
		// optimization for listing all files
		if ("**".equals(glob)) {
			Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
					FileMetadata meta = getFileMeta(path, listTombstones);
					if (meta != null) {
						list.add(meta);
					}
					return CONTINUE;
				}
			});
			return list;
		}
		// optimization for single-file requests
		if (!isWildcard(glob)) {
			Path path = resolve(glob);
			FileMetadata meta = getFileMeta(path, listTombstones);
			if (meta != null) {
				list.add(meta);
			}
			return list;
		}
		// common
		PathMatcher matcher = storage.getFileSystem().getPathMatcher("glob:" + glob);
		Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				if (matcher.matches(storage.relativize(path))) {
					FileMetadata meta = getFileMeta(path, listTombstones);
					if (meta != null) {
						list.add(meta);
					}
				}
				return CONTINUE;
			}
		});
		return list;
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
	public PromiseStats getListPromise() {
		return listPromise;
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
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getSingleDeletePromise() {
		return singleDeletePromise;
	}
	//endregion
}
