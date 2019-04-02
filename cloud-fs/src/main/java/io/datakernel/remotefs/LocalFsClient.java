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
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.async.Promise.ofBlockingCallable;
import static io.datakernel.remotefs.FileNamingScheme.FilenameInfo;
import static io.datakernel.remotefs.RemoteFsUtils.escapeGlob;
import static io.datakernel.remotefs.RemoteFsUtils.isWildcard;
import static io.datakernel.util.CollectionUtils.set;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * An implementation of {@link FsClient} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(LocalFsClient.class);

	public static final FileNamingScheme DEFAULT_FILE_NAMING_SCHEME = new FileNamingScheme() {
		private static final String SEPARATOR = "@";
		private static final String TOMBSTONE_QUALIFIER = "!";

		@Override
		public String encode(String name, long revision, boolean tombstone) {
			return name + SEPARATOR + (tombstone ? TOMBSTONE_QUALIFIER : "") + revision;
		}

		@Override
		public FilenameInfo decode(Path path, String name) {
			int idx = name.lastIndexOf(SEPARATOR);
			if (idx == -1) {
				return null;
			}
			String meta = name.substring(idx + 1);
			name = name.substring(0, idx);
			boolean tombstone = meta.startsWith(TOMBSTONE_QUALIFIER);
			if (tombstone) {
				meta = meta.substring(TOMBSTONE_QUALIFIER.length());
			}
			try {
				return new FilenameInfo(path, name, Long.parseLong(meta), tombstone);
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

	private ChannelConsumer<ByteBuf> doUpload(Path path, long size, long offset) throws StacklessException, IOException {
		if (offset > size) {
			throw OFFSET_TOO_BIG;
		}
		long skip = lazyOverrides ? size - offset : 0;
		return ChannelFileWriter.create(AsyncFile.open(executor, path, set(CREATE, WRITE), this))
				.withForceOnClose(true)
				.withOffset(offset + skip)
				.transformWith(ChannelByteRanger.drop(skip));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long offset, long revision) {
		checkNotNull(name, "name");
		checkArgument(offset >= 0, "offset < 0");

		return ofBlockingCallable(executor,
				() -> {
					if (defaultRevision != null) {
						Path path = resolve(name);
						long size;
						if (Files.isRegularFile(path)) {
							size = Files.size(path);
						} else {
							size = 0;
							Files.createDirectories(path.getParent());
						}
						return doUpload(path, size, offset);
					}

					FilenameInfo existing = getInfo(name);

					if (existing == null) {
						Path path = resolve(namingScheme.encode(name, revision, false));
						Files.createDirectories(path.getParent());
						return doUpload(path, 0, offset);
					}

					if (existing.getRevision() < revision) {

						// cleanup existing file/tombstone with lower revision
						Files.deleteIfExists(existing.getFilePath());

						return doUpload(resolve(namingScheme.encode(name, revision, false)), 0, offset);
					}

					if (existing.getRevision() == revision) {
						if (existing.isTombstone()) {
							return ChannelConsumers.<ByteBuf>recycling();
						}
						Path path = existing.getFilePath();
						return doUpload(path, Files.size(path), offset);
					}

					return ChannelConsumers.<ByteBuf>recycling();
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
					Path path;

					if (defaultRevision == null) {
						FilenameInfo info = getInfo(name);

						if (info == null || info.isTombstone()) {
							throw FILE_NOT_FOUND;
						}
						path = info.getFilePath();
					} else {
						path = resolve(name);
						if (!Files.exists(path)) {
							throw FILE_NOT_FOUND;
						}
					}

					return ChannelFileReader.readFile(AsyncFile.open(executor, path, ChannelFileReader.READ_OPTIONS, this))
							.withBufferSize(readerBufferSize)
							.withOffset(offset)
							.withLength(length == -1 ? Long.MAX_VALUE : length);
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
					// old logic
					if (defaultRevision != null) {
						if (removeRevision != defaultRevision) {
							throw UNSUPPORTED_REVISION;
						}
						Path path = resolve(name);
						Path targetPath = resolve(target);

						if (Files.isDirectory(path) || Files.isDirectory(targetPath)) {
							throw MOVING_DIRS;
						}
						// noop when paths are equal
						if (path.equals(targetPath)) {
							return null;
						}
						// cannot move into existing file
						if (Files.isRegularFile(targetPath)) {
							throw FILE_EXISTS;
						}

						if (Files.isRegularFile(path)) {
							Files.createDirectories(targetPath.getParent());
							Files.move(path, targetPath, ATOMIC_MOVE);
						} else {
							Files.deleteIfExists(targetPath);
						}
						return null;
					}
					doCopy(name, target, targetRevision);
					doDelete(name, removeRevision);
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "move", name, target, this))
				.whenComplete(singleMovePromise.recordStats());
	}

	@Override
	public Promise<Void> copy(String name, String target, long targetRevision) {
		return ofBlockingCallable(executor,
				() -> {
					doCopy(name, target, targetRevision);
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "copy", name, target, this))
				.whenComplete(singleCopyPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(String name, long revision) {
		return ofBlockingCallable(executor,
				() -> {
					doDelete(name, revision);
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
			FilenameInfo info = getInfo(name);
			return info != null ? toFileMetadata(info) : null;
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
		Path path;
		Path targetPath;

		// check the revisions
		if (defaultRevision != null) {
			if (targetRevision != defaultRevision) {
				throw UNSUPPORTED_REVISION;
			}

			path = resolve(name);
			if (!Files.isRegularFile(path)) {
				return;
			}
			targetPath = resolve(target);
		} else {
			FilenameInfo info = getInfo(name);
			if (info == null || info.isTombstone()) {
				return;
			}
			path = info.getFilePath();
			targetPath = resolve(namingScheme.encode(target, targetRevision, false));
		}

		if (Files.isDirectory(path) || Files.isDirectory(targetPath)) {
			throw MOVING_DIRS;
		}
		// noop when paths are equal
		if (path.equals(targetPath)) {
			return;
		}

		// with old logic we cannot move into existing file
		if (Files.isRegularFile(targetPath)) {
			throw FILE_EXISTS;
		}

		tryHardlinkOrCopy(path, targetPath);
	}

	private void doDelete(String name, long revision) throws IOException, StacklessException {

		// old logic - just delete the file
		if (defaultRevision != null) {
			if (revision != defaultRevision) {
				throw UNSUPPORTED_REVISION;
			}

			Path path = resolve(name);
			if (Files.isDirectory(path)) {
				throw MOVING_DIRS;
			}

			Files.deleteIfExists(path);
			return;
		}

		Path path = resolve(namingScheme.encode(name, revision, true));

		if (Files.isDirectory(path)) {
			throw MOVING_DIRS;
		}
		FilenameInfo existing = getInfo(name);

		if (existing == null) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
			return;
		}

		if (existing.isTombstone() && existing.getRevision() < revision || existing.getRevision() <= revision) {
			Files.deleteIfExists(existing.getFilePath());
			Files.createFile(path);
		}
	}

	private FileMetadata toFileMetadata(FilenameInfo info) {
		try {
			Path path = info.getFilePath();
			long timestamp = Files.getLastModifiedTime(path).toMillis();
			return info.isTombstone() ?
					FileMetadata.tombstone(info.getName(), timestamp, info.getRevision()) :
					FileMetadata.of(info.getName(), Files.size(path), timestamp, info.getRevision());
		} catch (Exception e) {
			logger.warn("error while getting metadata for file {}", info.getFilePath());
			return null;
		}
	}

	private List<FileMetadata> doList(String glob, boolean listTombstones) throws IOException, StacklessException {
		return findMatched(glob, listTombstones).stream()
				.map(this::toFileMetadata)
				.filter(Objects::nonNull)
				.collect(toList());
	}

	private FilenameInfo getBetterFilenameInfo(FilenameInfo first, FilenameInfo second) {
		return first.getRevision() > second.getRevision() ?
				first :
				second.getRevision() > first.getRevision() ?
						second :
						first.isTombstone() ?
								first :
								second;
	}

	@Nullable
	private FilenameInfo getInfo(String name) throws IOException, StacklessException {
		Iterator<FilenameInfo> matched = findMatched(escapeGlob(name), true).iterator();
		return matched.hasNext() ? matched.next() : null;
	}

	private Collection<FilenameInfo> findMatched(String glob, boolean includeTombstones) throws IOException, StacklessException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return emptyList();
		}

		if (defaultRevision != null) {
			return doListWithoutRevisions(glob);
		}

		Map<String, FilenameInfo> files = new HashMap<>();
		// optimization for listing all files
		if ("**".equals(glob)) {
			Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
					FilenameInfo info = namingScheme.decode(path, storage.relativize(path).toString());
					if (info != null && (includeTombstones || !info.isTombstone())) {
						files.merge(info.getName(), info, LocalFsClient.this::getBetterFilenameInfo);
					}
					return CONTINUE;
				}
			});
			return files.values();
		}

		StringBuilder folder = new StringBuilder();
		String[] split = glob.split("/");
		for (int i = 0; i < split.length - 1; i++) {
			String part = split[i];
			if (isWildcard(part)) {
				break;
			}
			folder.append(part).append('/');
		}

		PathMatcher matcher = storage.getFileSystem().getPathMatcher("glob:" + glob.substring(folder.length()));
		Files.walkFileTree(storage.resolve(folder.toString()), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
				FilenameInfo info = namingScheme.decode(path, storage.relativize(path).toString());
				if (info != null && (includeTombstones || !info.isTombstone()) && matcher.matches(Paths.get(info.getName()))) {
					files.merge(info.getName(), info, LocalFsClient.this::getBetterFilenameInfo);
				}
				return CONTINUE;
			}
		});
		return files.values();
	}

	private FilenameInfo infoFor(Path path, long revision) {
		return new FilenameInfo(path, storage.relativize(path).toString(), revision, false);
	}

	private List<FilenameInfo> doListWithoutRevisions(String glob) throws IOException, StacklessException {
		assert defaultRevision != null;

		List<FilenameInfo> list = new ArrayList<>();
		// optimization for listing all files
		if ("**".equals(glob)) {
			Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
					list.add(infoFor(path, defaultRevision));
					return CONTINUE;
				}
			});
			return list;
		}
		// optimization for single-file requests
		if (!isWildcard(glob)) {
			Path path = resolve(glob);
			if (Files.isRegularFile(path)) {
				list.add(infoFor(path, defaultRevision));
			}
			return list;
		}
		// common
		PathMatcher matcher = storage.getFileSystem().getPathMatcher("glob:" + glob);
		Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
				if (matcher.matches(storage.relativize(path))) {
					list.add(infoFor(path, defaultRevision));
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
