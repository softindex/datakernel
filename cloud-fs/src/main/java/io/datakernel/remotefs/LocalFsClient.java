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

import io.datakernel.async.service.EventloopService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.csp.process.ChannelByteRanger;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.promise.Promise;
import io.datakernel.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.remotefs.FileNamingScheme.FilenameInfo;
import static io.datakernel.remotefs.RemoteFsUtils.isWildcard;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * An implementation of {@link FsClient} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalFsClient implements FsClient, EventloopService, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(LocalFsClient.class);

	public static final FileNamingScheme REVISION_NAMING_SCHEME = new FileNamingScheme() {
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

	public static class NoopNamingScheme implements FileNamingScheme {
		private final long defaultRevision;

		public NoopNamingScheme(long defaultRevision) {
			this.defaultRevision = defaultRevision;
		}

		@Override
		public String encode(String name, long revision, boolean tombstone) {
			return name;
		}

		@Override
		public FilenameInfo decode(Path path, String name) {
			return new FilenameInfo(path, name, defaultRevision, false);
		}
	}

	public static final Duration DEFAULT_TOMBSTONE_TTL = Duration.ofHours(1);

	public static final char FILE_SEPARATOR_CHAR = '/';

	public static final String FILE_SEPARATOR = String.valueOf(FILE_SEPARATOR_CHAR);

	private static final Function<String, String> toLocalName = File.separatorChar == FILE_SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(FILE_SEPARATOR_CHAR, File.separatorChar);

	private static final Function<String, String> toRemoteName = File.separatorChar == FILE_SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(File.separatorChar, FILE_SEPARATOR_CHAR);

	private final Eventloop eventloop;
	private final Path storage;
	private final Executor executor;

	private MemSize readerBufferSize = MemSize.kilobytes(256);
	private boolean lazyOverrides = true;
	@Nullable
	private Long defaultRevision = DEFAULT_REVISION;

	private long tombstoneTtl = 0;

	private FileNamingScheme namingScheme = new NoopNamingScheme(DEFAULT_REVISION);

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
	private LocalFsClient(Eventloop eventloop, Path storage, Executor executor) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;

		now = eventloop;
	}

	public static LocalFsClient create(Eventloop eventloop, Executor executor, Path storageDir) {
		return new LocalFsClient(eventloop, storageDir, executor);
	}

	public static LocalFsClient create(Eventloop eventloop, Path storageDir) {
		return create(eventloop, Executors.newSingleThreadExecutor(), storageDir);
	}

	public LocalFsClient withLazyOverrides(boolean lazyOverrides) {
		this.lazyOverrides = lazyOverrides;
		return this;
	}

	public LocalFsClient withDefaultRevision(long defaultRevision) {
		this.defaultRevision = defaultRevision;
		this.namingScheme = new NoopNamingScheme(defaultRevision);
		this.tombstoneTtl = 0;
		return this;
	}

	public LocalFsClient withRevisions() {
		return withRevisions(REVISION_NAMING_SCHEME, DEFAULT_TOMBSTONE_TTL);
	}

	public LocalFsClient withRevisions(FileNamingScheme namingScheme, Duration tombstoneTtl) {
		this.defaultRevision = null;
		this.namingScheme = namingScheme;
		this.tombstoneTtl = tombstoneTtl.toMillis();
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

	private Promise<ChannelConsumer<ByteBuf>> doUpload(Path path, long size, long offset) throws StacklessException, IOException {
		if (offset > size) {
			throw OFFSET_TOO_BIG;
		}
		long skip = lazyOverrides ? size - offset : 0;

		FileChannel channel = FileChannel.open(path, set(CREATE, WRITE));
		return Promise.of(ChannelFileWriter.create(executor, channel)
				.withOffset(offset + skip)
				.transformWith(ChannelByteRanger.drop(skip)));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long offset, long revision) {
		checkArgument(offset >= 0, "offset < 0");
		checkArgument(defaultRevision == null || revision == defaultRevision, "unsupported revision");

		return Promise.ofBlockingCallable(executor, () -> getInfo(name))
				.then(existing -> {
					try {
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
								return Promise.of(ChannelConsumers.<ByteBuf>recycling());
							}
							Path path = existing.getFilePath();
							return doUpload(path, Files.size(path), offset);
						}

						return Promise.of(ChannelConsumers.<ByteBuf>recycling());
					} catch (StacklessException | IOException e) {
						return Promise.ofException(e);
					}
				}).map(consumer -> consumer
						// calling withAcknowledgement in eventloop thread
						.withAcknowledgement(ack -> ack
								.whenComplete(writeFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "writing to file", name, offset, revision, this))))
				.whenComplete(writeBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "upload", name, offset, revision, this));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		checkArgument(offset >= 0, "offset < 0");
		checkArgument(length >= -1, "length < -1");

		return Promise.ofBlockingCallable(executor,
				() -> {
					FilenameInfo info = getInfo(name);
					if (info == null || info.isTombstone()) {
						throw FILE_NOT_FOUND;
					}
					return info;
				})
				.then(info -> ChannelFileReader.open(executor, info.getFilePath()))
				.map(consumer -> consumer
						.withBufferSize(readerBufferSize)
						.withOffset(offset)
						.withLength(length == -1 ? Long.MAX_VALUE : length)
						// call withAcknowledgement in eventloop thread
						.withEndOfStream(eos -> eos.whenComplete(readFinishPromise.recordStats())))
				.whenComplete(toLogger(logger, TRACE, "download", name, offset, length, this))
				.whenComplete(readBeginPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(@NotNull String glob) {
		return Promise.ofBlockingCallable(executor, () -> doList(glob, true))
				.whenComplete(toLogger(logger, TRACE, "listEntities", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return Promise.ofBlockingCallable(executor, () -> doList(glob, false))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target, long targetRevision, long tombstoneRevision) {
		checkArgument(defaultRevision == null || targetRevision == defaultRevision, "unsupported revision");
		checkArgument(defaultRevision == null || tombstoneRevision == defaultRevision, "unsupported revision");

		return Promise.ofBlockingCallable(executor,
				() -> {
					if (defaultRevision == null) {
						doCopy(name, target, targetRevision);
						doDelete(name, tombstoneRevision);
						return (Void) null;
					}

					// old logic (optimization that uses atomic moves)
					if (tombstoneRevision != defaultRevision) {
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
				})
				.whenComplete(toLogger(logger, TRACE, "move", name, target, this))
				.whenComplete(singleMovePromise.recordStats());
	}

	@Override
	public Promise<Void> moveDir(@NotNull String name, @NotNull String target, long targetRevision, long removeRevision) {
		if (defaultRevision == null) {
			return FsClient.super.moveDir(name, target, targetRevision, removeRevision);
		}
		String finalName = name.endsWith("/") ? name : name + '/';
		String finalTarget = target.endsWith("/") ? target : target + '/';

		Path from, to;
		try {
			from = resolve(finalName);
			to = resolve(finalTarget);
		} catch (StacklessException e) {
			return Promise.ofException(e);
		}

		return Promise.ofBlockingCallable(executor, () -> Files.isRegularFile(to))
				.then(isRegular -> {
					if (isRegular) {
						return Promise.ofException(FILE_EXISTS);
					}
					return Promise.ofBlockingCallable(executor, () -> Files.isDirectory(to));
				})
				.then(isDir -> {
					if (isDir) {
						return FsClient.super.moveDir(name, target, targetRevision, removeRevision);
					}
					return Promise.ofBlockingCallable(executor, () -> {
						if (!Files.isDirectory(from)) {
							return null;
						}
						try {
							Files.move(from, to, ATOMIC_MOVE);
						} catch (AtomicMoveNotSupportedException e) {
							Files.move(from, to);
						}
						return null;
					});
				});
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target, long targetRevision) {
		checkArgument(defaultRevision == null || targetRevision == defaultRevision, "unsupported revision");

		return Promise.ofBlockingCallable(executor,
				() -> {
					doCopy(name, target, targetRevision);
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "copy", name, target, this))
				.whenComplete(singleCopyPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name, long revision) {
		checkArgument(defaultRevision == null || revision == defaultRevision, "unsupported revision");

		return Promise.ofBlockingCallable(executor,
				() -> {
					doDelete(name, revision);
					return (Void) null;
				})
				.whenComplete(toLogger(logger, TRACE, "delete", name, this))
				.whenComplete(singleDeletePromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete(); // local fs is always available
	}

	@Override
	public Promise<FileMetadata> getMetadata(@NotNull String name) {
		return Promise.ofBlockingCallable(executor, () -> {
			FilenameInfo info = getInfo(name);
			return info != null ? toFileMetadata(info) : null;
		});
	}

	@Override
	public FsClient subfolder(@NotNull String folder) {
		if (folder.length() == 0) {
			return this;
		}
		try {
			LocalFsClient client = new LocalFsClient(eventloop, resolve(folder), executor);
			client.readerBufferSize = readerBufferSize;
			client.lazyOverrides = lazyOverrides;
			client.defaultRevision = defaultRevision;
			client.tombstoneTtl = tombstoneTtl;
			client.namingScheme = namingScheme;
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
	public Promise<Void> start() {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try {
						Files.createDirectories(storage);
					} catch (IOException e) {
						throw new UncheckedException(e);
					}
				})
				.then(this::cleanup);
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	public Promise<Void> cleanup() {
		return Promise.ofBlockingCallable(executor, () -> {
			long border = now.currentTimeMillis() - tombstoneTtl;
			findMatching("**", true).stream()
					.filter(FilenameInfo::isTombstone)
					.forEach(info -> {
						long ts;
						try {
							ts = Files.getLastModifiedTime(info.getFilePath()).toMillis();
						} catch (IOException e) {
							logger.warn("Failed to get timestamp of the tombstone {}", info.getName());
							return;
						}
						if (ts < border) {
							try {
								Files.deleteIfExists(info.getFilePath());
							} catch (IOException e) {
								logger.warn("Failed clean up expired tombstone {}", info.getName());
							}
						}
					});
			return null;
		});
	}

	public Promise<Void> remove(String name) {
		return Promise.ofBlockingCallable(executor, () -> {
			FilenameInfo info = getInfo(name);
			if (info != null) {
				Files.deleteIfExists(info.getFilePath());
			}
			return null;
		});
	}

	@Override
	public String toString() {
		return "LocalFsClient{storage=" + storage + '}';
	}

	private Path resolve(String name) throws StacklessException {
		Path path = storage.resolve(toLocalName.apply(name)).normalize();
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
			// if couldn't, then just actually copy it
			Files.copy(path, targetPath);
		}
	}

	private void doCopy(String name, String target, long targetRevision) throws StacklessException, IOException {
		FilenameInfo info = getInfo(name);
		if (info == null || info.isTombstone()) {
			return;
		}
		Path path = info.getFilePath();
		Path targetPath = resolve(namingScheme.encode(target, targetRevision, false));

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
		Path path = resolve(namingScheme.encode(name, revision, true));

		if (Files.isDirectory(path)) {
			throw MOVING_DIRS;
		}
		FilenameInfo existing = getInfo(name);

		if (existing == null) {
			if (tombstoneTtl > 0) {
				Files.createDirectories(path.getParent());
				Files.createFile(path);
			}
			return;
		}

		if (existing.isTombstone() ? existing.getRevision() < revision : existing.getRevision() <= revision) {
			Files.deleteIfExists(existing.getFilePath());
			if (tombstoneTtl > 0) {
				Files.createFile(path);
			}
		}
	}

	@Nullable
	private FilenameInfo getInfo(String name) throws IOException, StacklessException {
		if (defaultRevision != null) {
			Path path = resolve(name);
			if (Files.isRegularFile(path)) {
				return new FilenameInfo(path, name, defaultRevision, false);
			}
			return null;
		}

		int idx = name.lastIndexOf(FILE_SEPARATOR_CHAR);
		Path folder = idx != -1 ? resolve(name.substring(0, idx)) : storage;

		Map<String, FilenameInfo> files = new HashMap<>();

		walkFiles(folder, path -> {
			FilenameInfo info = namingScheme.decode(path, storage.relativize(path).toString());
			if (info != null && info.getName().equals(name)) {
				files.merge(info.getName(), info, LocalFsClient.this::getBetterFilenameInfo);
			}
		});

		Iterator<FilenameInfo> matched = files.values().iterator();
		return matched.hasNext() ? matched.next() : null;
	}

	private List<FileMetadata> doList(String glob, boolean includeTombstones) throws IOException, StacklessException {
		return findMatching(glob, includeTombstones).stream()
				.map(this::toFileMetadata)
				.filter(Objects::nonNull)
				.collect(toList());
	}

	private Collection<FilenameInfo> findMatching(String glob, boolean includeTombstones) throws IOException, StacklessException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return emptyList();
		}

		// get strict prefix folder from the glob
		StringBuilder sb = new StringBuilder();
		String[] split = glob.split(FILE_SEPARATOR);
		for (int i = 0; i < split.length - 1; i++) {
			String part = split[i];
			if (isWildcard(part)) {
				break;
			}
			sb.append(part).append(FILE_SEPARATOR_CHAR);
		}
		String subglob = glob.substring(sb.length());
		Path subfolder = resolve(sb.toString());

		return defaultRevision != null ?
				simpleFindMatching(subfolder, subglob) :
				findMatchingWithRevision(subfolder, subglob, includeTombstones);
	}

	private FilenameInfo simpleFileInfo(Path path) {
		assert defaultRevision != null;

		return new FilenameInfo(path, storage.relativize(path).toString(), defaultRevision, false);
	}

	private Collection<FilenameInfo> simpleFindMatching(Path folder, String glob) throws IOException {
		assert defaultRevision != null;

		// optimization for listing all files
		if ("**".equals(glob)) {
			List<FilenameInfo> list = new ArrayList<>();
			walkFiles(folder, path -> list.add(simpleFileInfo(path)));
			return list;
		}

		// optimization for single-file requests
		if ("".equals(glob)) {
			return Files.isRegularFile(folder) ?
					singletonList(simpleFileInfo(folder)) :
					emptyList();
		}

		// common route
		List<FilenameInfo> list = new ArrayList<>();
		PathMatcher matcher = storage.getFileSystem().getPathMatcher("glob:" + glob);

		walkFiles(folder, glob, path -> {
			if (matcher.matches(folder.relativize(path))) {
				list.add(simpleFileInfo(path));
			}
		});

		return list;
	}

	private Collection<FilenameInfo> findMatchingWithRevision(Path folder, String glob, boolean includeTombstones) throws IOException {
		Map<String, FilenameInfo> files = new HashMap<>();

		// optimization for listing all files
		if ("**".equals(glob)) {
			walkFiles(folder, path -> {
				FilenameInfo info = namingScheme.decode(path, storage.relativize(path).toString());
				if (info != null && (includeTombstones || !info.isTombstone())) {
					files.merge(info.getName(), info, LocalFsClient.this::getBetterFilenameInfo);
				}
			});
			return files.values();
		}

		// optimization for single-file requests
		if ("".equals(glob)) {
			walkFiles(folder, path -> {
				FilenameInfo info = namingScheme.decode(path, storage.relativize(path).toString());
				if (info != null && (!info.isTombstone() || includeTombstones)) {
					files.merge(info.getName(), info, LocalFsClient.this::getBetterFilenameInfo);
				}
			});
		}

		// common route
		PathMatcher matcher = storage.getFileSystem().getPathMatcher("glob:" + glob);

		int relativeSubfolderLength =
				storage.equals(folder) ?
						0 :
						storage.relativize(folder).toString().length() + 1;

		walkFiles(folder, glob, path -> {
			FilenameInfo info = namingScheme.decode(path, storage.relativize(path).toString());
			if (info == null || (info.isTombstone() && !includeTombstones)) {
				return;
			}
			String name = info.getName();
			if (matcher.matches(Paths.get(name.substring(relativeSubfolderLength)))) {
				files.merge(name, info, LocalFsClient.this::getBetterFilenameInfo);
			}
		});

		return files.values();
	}

	private FileMetadata toFileMetadata(FilenameInfo info) {
		try {
			String name = toRemoteName.apply(info.getName());
			Path path = info.getFilePath();
			long timestamp = Files.getLastModifiedTime(path).toMillis();
			return info.isTombstone() ?
					FileMetadata.tombstone(name, timestamp, info.getRevision()) :
					FileMetadata.of(name, Files.size(path), timestamp, info.getRevision());
		} catch (Exception e) {
			logger.warn("error while getting metadata for file {}", info.getFilePath());
			return null;
		}
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

	@FunctionalInterface
	interface Walker {

		void accept(Path path) throws IOException;
	}

	private void walkFiles(Path dir, Walker walker) throws IOException {
		walkFiles(dir, null, walker);
	}

	private void walkFiles(Path dir, @Nullable String glob, Walker walker) throws IOException {
		if (!Files.isDirectory(dir)) {
			return;
		}
		String[] parts;
		if (glob == null || (parts = glob.split(FILE_SEPARATOR))[0].contains("**")) {
			Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					walker.accept(file);
					return CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path file, IOException exc) {
					logger.warn("Failed to visit file {}", storage.relativize(file), exc);
					return CONTINUE;
				}
			});
			return;
		}

		FileSystem fs = dir.getFileSystem();

		PathMatcher[] matchers = new PathMatcher[parts.length];
		matchers[0] = fs.getPathMatcher("glob:" + parts[0]);

		for (int i = 1; i < parts.length; i++) {
			String part = parts[i];
			if (part.contains("**")) {
				break;
			}
			matchers[i] = fs.getPathMatcher("glob:" + part);
		}

		Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				walker.accept(file);
				return CONTINUE;
			}

			@Override
			public FileVisitResult preVisitDirectory(Path subdir, BasicFileAttributes attrs) {
				if (subdir.equals(dir)) {
					return CONTINUE;
				}
				Path relative = dir.relativize(subdir);
				for (int i = 0; i < relative.getNameCount(); i++) {
					PathMatcher matcher = matchers[i];
					if (matcher == null) {
						return CONTINUE;
					}
					if (!matcher.matches(relative.getName(i))) {
						return SKIP_SUBTREE;
					}
				}
				return CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) {
				logger.warn("Failed to visit file {}", storage.relativize(file), exc);
				return CONTINUE;
			}
		});
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
