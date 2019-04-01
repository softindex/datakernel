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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.async.Promise.ofBlockingCallable;
import static io.datakernel.remotefs.RemoteFsUtils.checkRange;
import static io.datakernel.remotefs.RemoteFsUtils.escapeGlob;
import static io.datakernel.util.CollectionUtils.set;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptyList;

/**
 * An implementation of {@link FsClient} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(LocalFsClient.class);

	public static final FileNamingScheme DEFAULT_FILE_NAMING_SCHEME = new FileNamingScheme() {
		@Override
		public String encode(String name, long revision, boolean tombstone) {
			return name + (tombstone ? "#!" : "#") + revision;
		}

		@Override
		@Nullable
		public FileMetadata decode(String filename, long size, long timestamp) {
			int idx = filename.lastIndexOf('#');
			if (idx == -1) {
				return null;
			}
			String meta = filename.substring(idx + 1);
			filename = filename.substring(0, idx);
			boolean tombstone = meta.startsWith("!");
			if (tombstone) {
				meta = meta.substring(1);
			}
			try {
				long revision = Long.parseLong(meta);
				return tombstone ?
						FileMetadata.tombstone(filename, timestamp, revision) :
						FileMetadata.of(filename, size, timestamp, revision);
			} catch (NumberFormatException ignored) {
				return null;
			}
		}
	};

	private final Eventloop eventloop;
	private final Path storage;
	private final Executor executor;
	private final Object lock;

	private MemSize readerBufferSize = MemSize.kilobytes(256);
	private boolean lazyOverrides = true;
	@Nullable
	private Long defaultRevision = DEFAULT_REVISION;

	private FileNamingScheme namingScheme = DEFAULT_FILE_NAMING_SCHEME;

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

	public LocalFsClient withNamingScheme(FileNamingScheme namingScheme) {
		this.namingScheme = namingScheme;
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
						Path path = resolve(defaultRevision == null ? namingScheme.encode(name, revision, false) : name);
						boolean exists = Files.isRegularFile(path);
						long size = exists ? Files.size(path) : 0;
						if (offset > size) {
							throw OFFSET_TOO_BIG;
						}
						if (!exists) {
							Files.createDirectories(path.getParent());
						}
						AsyncFile file = AsyncFile.open(executor, path, set(CREATE, WRITE), this);
						long skip = lazyOverrides ? size - offset : 0;

						return ChannelFileWriter.create(file)
								.withForceOnClose(true)
								.withOffset(offset + skip)
								.transformWith(ChannelByteRanger.drop(skip));
					}
				})
				.map(consumer -> consumer
						// calling withAcknowledgement in eventloop thread
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
						List<FileMetadata> found = doList(escapeGlob(name), true);
						FileMetadata meta = found.isEmpty() ? null : found.get(0);

						if (meta == null || meta.isTombstone()) {
							throw FILE_NOT_FOUND;
						}

						checkRange(meta.getSize(), offset, length);

						Path path = resolve(defaultRevision == null ? namingScheme.encode(name, meta.getRevision(), false) : name);

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
						if (defaultRevision != null && removeRevision != defaultRevision) {
							throw UNSUPPORTED_REVISION;
						}
						doCopy(name, target, targetRevision);
						doDelete(resolve(name), removeRevision);
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
						doCopy(name, target, targetRevision);
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
		return ofBlockingCallable(executor, () -> {
			List<FileMetadata> found = doList(escapeGlob(name), true);
			return found.isEmpty() ? null : found.get(0);
		});
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

	private void tryHardlinkOrCopy(Path path, Path targetPath) throws IOException {
		if (!Files.deleteIfExists(targetPath)) {
			Files.createDirectories(targetPath.getParent());
		}
		try {
			// try to create a hardlink
			Files.createLink(targetPath, path);
		} catch (UnsupportedOperationException | SecurityException e) {
			// if couldnt, then just actually copy it
			Files.copy(path, targetPath);
		}
	}

	private void doCopy(String name, String target, long targetRevision) throws StacklessException, IOException {
		Path path = resolve(name);
		Path targetPath = resolve(target);

		if (Files.isDirectory(path) || Files.isDirectory(targetPath)) {
			throw MOVING_DIRS;
		}
		// noop when paths are equal
		if (path.equals(targetPath)) {
			return;
		}

		// check the revisions
		if (defaultRevision != null) {
			if (targetRevision != defaultRevision) {
				throw UNSUPPORTED_REVISION;
			}
			// with old logic we cannot move into existing file
			if (Files.isRegularFile(targetPath)) {
				throw FILE_EXISTS;
			}
			// copying 'nothing' into anything is a noop
			if (Files.isRegularFile(path)) {
				tryHardlinkOrCopy(path, targetPath);
			}
			return;
		}

		List<FileMetadata> found = doList(escapeGlob(name), true);

		// copying 'nothing' into anything is a noop
		FileMetadata pathMeta = found.isEmpty() ? null : found.get(0);
		if (pathMeta == null || pathMeta.isTombstone()) {
			return;
		}

		targetPath = resolve(namingScheme.encode(targetPath.toString(), targetRevision, false));

		tryHardlinkOrCopy(resolve(namingScheme.encode(pathMeta.getName(), pathMeta.getRevision(), false)), targetPath);
	}

	private void doDelete(Path path, long revision) throws IOException, StacklessException {
		if (Files.isDirectory(path)) {
			throw MOVING_DIRS;
		}
		if (defaultRevision == null) {
			Path tombstone = resolve(namingScheme.encode(storage.relativize(path).toString(), revision, true));
			Files.createDirectories(tombstone.getParent());
			Files.createFile(tombstone);
			return;
		}
		// old logic - just delete the file
		if (revision != defaultRevision) {
			throw UNSUPPORTED_REVISION;
		}
		Files.deleteIfExists(path);
	}

	@Nullable
	private FileMetadata getFileMeta(Path path, String name) throws IOException {
		synchronized (lock) {
			if (!Files.isRegularFile(path)) {
				return null;
			}
			name = File.separatorChar == '\\' ? name.replace('\\', '/') : name;
			return defaultRevision == null ?
					namingScheme.decode(name, Files.size(path), Files.getLastModifiedTime(path).toMillis()) :
					FileMetadata.of(name, Files.size(path), Files.getLastModifiedTime(path).toMillis(), defaultRevision);
		}
	}

	private List<FileMetadata> doList(String glob, boolean listTombstones) throws IOException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return emptyList();
		}
		Map<String, FileMetadata> files = new HashMap<>();
		// optimization for listing all files
		if ("**".equals(glob)) {
			Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
					FileMetadata meta = getFileMeta(path, storage.relativize(path).toString());
					if (meta != null && (listTombstones || !meta.isTombstone())) {
						files.merge(meta.getName(), meta, FileMetadata::getMoreCompleteFile);
					}
					return CONTINUE;
				}
			});
			return new ArrayList<>(files.values());
		}
		int counter = 0;
		for (String part : glob.split("/")) {
			if (RemoteFsUtils.isWildcard(part)) {
				break;
			}
			counter++;
		}
		int concreteDirCount = counter;
		PathMatcher matcher = storage.getFileSystem().getPathMatcher("glob:" + glob);
		Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
				Path rel = storage.relativize(dir);
				return rel.getNameCount() <= concreteDirCount && !glob.startsWith(rel.toString()) ? SKIP_SUBTREE : CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				FileMetadata meta = getFileMeta(path, storage.relativize(path).toString());
				if (meta != null && (listTombstones || !meta.isTombstone()) && matcher.matches(Paths.get(meta.getName()))) {
					files.merge(meta.getName(), meta, FileMetadata::getMoreCompleteFile);
				}
				return CONTINUE;
			}
		});
		return new ArrayList<>(files.values());
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
