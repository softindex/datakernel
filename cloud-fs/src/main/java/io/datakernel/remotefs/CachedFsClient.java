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

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.async.function.AsyncSuppliers;
import io.datakernel.async.service.EventloopService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.ref.RefLong;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.Preconditions.checkState;

/**
 * Represents a cached filesystem client which is an implementation of {@link FsClient}.
 * Cached filesystem client works on top of two {@link FsClient}s.
 * First is main client, which is potentially slow, but contains necessary data. Typically it's a {@link RemoteFsClient}
 * which connects to a remote server.
 * It is backed up by second one, which acts as a cache folder, typically it is a local filesystem client ({@link LocalFsClient})
 * Cache replacement policy is defined by supplying a {@link Comparator} of {@link FullCacheStat}.
 */
public final class CachedFsClient implements FsClient, EventloopService {
	private static final double LOAD_FACTOR = 0.75;
	private final Eventloop eventloop;
	private final FsClient mainClient;
	private final FsClient cacheClient;
	private final Map<String, CacheStat> cacheStats = new HashMap<>();
	private final Comparator<FullCacheStat> comparator;
	private final AsyncSupplier<Void> ensureSpace = AsyncSuppliers.reuse(this::doEnsureSpace);
	private MemSize cacheSizeLimit;
	private long downloadingNowSize;
	private long totalCacheSize;
	CurrentTimeProvider timeProvider = CurrentTimeProvider.ofSystem();

	// region creators
	private CachedFsClient(Eventloop eventloop, FsClient mainClient, FsClient cacheClient, Comparator<FullCacheStat> comparator) {
		this.eventloop = eventloop;
		this.mainClient = mainClient;
		this.cacheClient = cacheClient;
		this.comparator = comparator;
	}

	public static CachedFsClient create(FsClient mainClient, FsClient cacheClient, Comparator<FullCacheStat> comparator) {
		return new CachedFsClient(Eventloop.getCurrentEventloop(), mainClient, cacheClient, comparator);
	}

	public CachedFsClient with(@NotNull MemSize cacheSizeLimit) {
		this.cacheSizeLimit = cacheSizeLimit;
		return this;
	}

	public CachedFsClient with(@NotNull CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		return this;
	}
	// endregion

	public Promise<Void> setCacheSizeLimit(@NotNull MemSize cacheSizeLimit) {
		this.cacheSizeLimit = cacheSizeLimit;
		return ensureSpace();
	}

	public MemSize getCacheSizeLimit() {
		return cacheSizeLimit;
	}

	public Promise<MemSize> getTotalCacheSize() {
		return cacheClient.list("**")
				.then(list -> Promise.of(MemSize.of(list.stream().mapToLong(FileMetadata::getSize).sum())));
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		checkState(cacheSizeLimit != null, "Cannot start cached client without specifying cache size limit");
		return getTotalCacheSize()
				.then(size -> {
					totalCacheSize = size.toLong();
					return ensureSpace();
				});
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long offset) {
		return mainClient.upload(name, offset);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long offset, long revision) {
		return mainClient.upload(name, offset, revision);
	}

	/**
	 * Tries to download file either from cache (if present) or from server.
	 *
	 * @param name   name of the file to be downloaded
	 * @param offset from which byte to download the file
	 * @param length how much bytes of the file do download
	 * @return promise for stream supplier of byte buffers
	 */
	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		checkArgument(offset >= 0, "Data offset must be greater than or equal to zero");
		checkArgument(length >= -1, "Data length must be either -1 or greater than or equal to zero");

		return cacheClient.getMetadata(name)
				.then(cacheMetadata -> {
					if (cacheMetadata == null) {
						if (offset != 0) {
							return mainClient.download(name, offset, length);
						}
						return mainClient.getMetadata(name)
								.then(mainMetadata -> {
									if (mainMetadata == null) {
										return Promise.ofException(new StacklessException(CachedFsClient.class, "File not found: " + name));
									}
									return downloadToCache(name, offset, length, 0, mainMetadata.getSize());
								});
					}
					long sizeInCache = cacheMetadata.getSize();

					if (length != -1 && sizeInCache >= offset + length) {
						return cacheClient.download(name, offset, length)
								.whenComplete(() -> updateCacheStats(name));
					}

					if (offset > sizeInCache) {
						return mainClient.download(name, offset, length);
					}

					return mainClient.getMetadata(name)
							.then(mainMetadata -> {
								if (mainMetadata == null) {
									return cacheClient.download(name, offset, length)
											.whenComplete(() -> updateCacheStats(name));
								}

								long sizeInMain = mainMetadata.getSize();

								if (sizeInCache >= sizeInMain) {
									return cacheClient.download(name, offset, length)
											.whenComplete(() -> updateCacheStats(name));
								}

								if ((length != -1) && (sizeInMain < (offset + length))) {
									String repr = name + "(size=" + sizeInMain + (offset != 0 ? ", offset=" + offset : "") + ", length=" + length;
									return Promise.ofException(new StacklessException(CachedFsClient.class, "Boundaries exceed file size: " + repr));
								}

								return downloadToCache(name, offset, length, sizeInCache, sizeInMain);
							});

				});
	}

	private Promise<ChannelSupplier<ByteBuf>> downloadToCache(String fileName, long offset, long length, long sizeInCache, long sizeInMain) {
		long size = length == -1 ? length : length + offset - sizeInCache;
		return mainClient.download(fileName, sizeInCache, size)
				.then(supplier -> {
					long toBeCached = sizeInMain - sizeInCache;
					if (downloadingNowSize + toBeCached > cacheSizeLimit.toLong() || toBeCached > cacheSizeLimit.toLong() * (1 - LOAD_FACTOR)) {
						return Promise.of(supplier);
					}
					downloadingNowSize += toBeCached;

					return ensureSpace()
							.map($ -> {
								ChannelSplitter<ByteBuf> splitter = ChannelSplitter.create(supplier);
								splitter.addOutput()
										.set(ChannelConsumer.ofPromise(cacheClient.upload(fileName, sizeInCache)));
								ChannelSupplier<ByteBuf> prefix = sizeInCache != 0 ?
										ChannelSupplier.ofPromise(cacheClient.download(fileName, offset, sizeInCache)) :
										ChannelSupplier.of();

								return ChannelSuppliers.concat(prefix, splitter.addOutput().getSupplier())
										.withEndOfStream(eos -> eos
												.both(splitter.getProcessCompletion())
												.then($2 -> {
													totalCacheSize += toBeCached;
													return updateCacheStats(fileName);
												})
												.whenResult($2 -> downloadingNowSize -= toBeCached));
							});
				});
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target, long targetRevision, long tombstoneRevision) {
		return mainClient.move(name, target, targetRevision, tombstoneRevision);
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target, long targetRevision) {
		return mainClient.copy(name, target, targetRevision);
	}

	/**
	 * Lists files that are matched by glob. List is combined from cache folder files and files that are on server.
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return promise that is a union of the most actual files from cache folder and server
	 */
	@Override
	public Promise<List<FileMetadata>> listEntities(@NotNull String glob) {
		return Promises.toList(cacheClient.listEntities(glob), mainClient.listEntities(glob))
				.map(lists -> FileMetadata.flatten(lists.stream()));
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return Promises.toList(cacheClient.list(glob), mainClient.list(glob))
				.map(lists -> FileMetadata.flatten(lists.stream()));
	}

	/**
	 * Deletes file both on server and in cache folder
	 *
	 * @return promise of {@link Void} that represents successful deletion
	 */
	@Override
	public Promise<Void> delete(@NotNull String name, long revision) {
		cacheStats.remove(name);
		return Promises.all(cacheClient.delete(name, revision), mainClient.delete(name, revision));
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return ensureSpace();
	}

	private Promise<Void> updateCacheStats(String fileName) {
		return Promise.of(cacheStats
				.computeIfPresent(fileName, (s, cacheStat) -> {
					cacheStat.numberOfHits++;
					cacheStat.lastHitTimestamp = timeProvider.currentTimeMillis();
					return cacheStat;
				}))
				.whenResult(() -> cacheStats.computeIfAbsent(fileName, s -> new CacheStat(1, timeProvider.currentTimeMillis())))
				.toVoid();
	}

	private Promise<Void> ensureSpace() {
		return ensureSpace.get();
	}

	@NotNull
	private Promise<Void> doEnsureSpace() {
		if (totalCacheSize + downloadingNowSize <= cacheSizeLimit.toLong()) {
			return Promise.complete();
		}
		RefLong size = new RefLong(0);
		return cacheClient.list("**")
				.map(list -> list
						.stream()
						.map(metadata -> {
							CacheStat cacheStat = cacheStats.get(metadata.getName());
							return cacheStat == null ?
									new FullCacheStat(metadata, 0, 0) :
									new FullCacheStat(metadata, cacheStat.numberOfHits, cacheStat.lastHitTimestamp);
						})
						.sorted(comparator.reversed())
						.filter(fullCacheStat -> size.inc(fullCacheStat.getFileMetadata().getSize()) > cacheSizeLimit.toLong() * LOAD_FACTOR))
				.then(filesToDelete -> Promises.all(filesToDelete
						.map(fullCacheStat -> cacheClient
								.delete(fullCacheStat.getFileMetadata().getName())
								.whenResult($ -> {
									totalCacheSize -= fullCacheStat.getFileMetadata().getSize();
									cacheStats.remove(fullCacheStat.getFileMetadata().getName());
								}))));
	}

	private static final class CacheStat {
		private long numberOfHits;
		private long lastHitTimestamp;

		private CacheStat(long numberOfHits, long lastHitTimestamp) {
			this.numberOfHits = numberOfHits;
			this.lastHitTimestamp = lastHitTimestamp;
		}

		@Override
		public String toString() {
			return "CacheStat{" +
					"numberOfHits=" + numberOfHits +
					", lastHitTimestamp=" + lastHitTimestamp +
					'}';
		}
	}

	/**
	 * POJO class that encapsulates stats about file in cache folder.
	 * Consists of {@link FileMetadata}, number of successful cache hits and a time of the last cache hit occurrence.
	 */
	public static final class FullCacheStat {
		private final FileMetadata fileMetadata;
		private final long numberOfHits;
		private final long lastHitTimestamp;

		FullCacheStat(FileMetadata fileMetadata, long numberOfHits, long lastHitTimestamp) {
			this.fileMetadata = fileMetadata;
			this.numberOfHits = numberOfHits;
			this.lastHitTimestamp = lastHitTimestamp;
		}

		public FileMetadata getFileMetadata() {
			return fileMetadata;
		}

		public long getNumberOfHits() {
			return numberOfHits;
		}

		public long getLastHitTimestamp() {
			return lastHitTimestamp;
		}

		@Override
		public String toString() {
			return "FullCacheStat{" +
					"fileMetadata=" + fileMetadata +
					", numberOfHits=" + numberOfHits +
					", lastHitTimestamp=" + lastHitTimestamp +
					'}';
		}
	}

	/**
	 * Default {@link Comparator} to compare files by the time of last usage
	 *
	 * @return LRU (Least recently used) {@link Comparator}
	 */
	public static Comparator<FullCacheStat> lruCompare() {
		return Comparator.comparing(FullCacheStat::getLastHitTimestamp)
				.thenComparing(FullCacheStat::getNumberOfHits)
				.thenComparing(fullCacheStat -> -fullCacheStat.getFileMetadata().getSize());
	}

	/**
	 * Default {@link Comparator} to compare files by the number of usages
	 *
	 * @return LFU (Least frequently used) {@link Comparator}
	 */
	public static Comparator<FullCacheStat> lfuCompare() {
		return Comparator.comparing(FullCacheStat::getNumberOfHits)
				.thenComparing(FullCacheStat::getLastHitTimestamp)
				.thenComparing(fullCacheStat -> -fullCacheStat.getFileMetadata().getSize());
	}

	/**
	 * Default {@link Comparator} to compare files by size
	 *
	 * @return filesize {@link Comparator}
	 */
	public static Comparator<FullCacheStat> sizeCompare() {
		return Comparator.comparingLong(fullCacheStat -> -fullCacheStat.getFileMetadata().getSize());
	}

	@Override
	public String toString() {
		return "CachedFsClient{mainClient=" + mainClient + ", cacheClient=" + cacheClient
				+ ", cacheSizeLimit = " + cacheSizeLimit.format() + '}';
	}
}
