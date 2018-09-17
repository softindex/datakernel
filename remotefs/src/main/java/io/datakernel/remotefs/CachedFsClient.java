package io.datakernel.remotefs;

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSuppliers;
import io.datakernel.serial.processor.SerialSplitter;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.MemSize;

import java.util.*;

import static io.datakernel.util.Preconditions.*;

/**
 * Represents a cached filesystem client which is an implementation of {@link FsClient}.
 * Cached filesystem client works on top of two {@link FsClient}s.
 * First is main client, which is potentially slow, but contains nessesary data. Typically it's a {@link RemoteFsClient}
 * which connects to a remote server.
 * It is backed up by second one, which acts as a cache folder, typically it is a local filesystem client ({@link LocalFsClient})
 * Cache replacement policy is defined by supplying a {@link Comparator} of {@link FullCacheStat}.
 */
public class CachedFsClient implements FsClient, EventloopService {
	private final Eventloop eventloop;
	private final FsClient mainClient;
	private final FsClient cacheClient;
	private MemSize cacheSizeLimit;
	private final Map<String, CacheStat> cacheStats = new HashMap<>();
	private final Comparator<FullCacheStat> comparator;
	CurrentTimeProvider timeProvider = CurrentTimeProvider.ofSystem();
	private final AsyncSupplier<Void> ensureSpace = AsyncSuppliers.reuse(this::doEnsureSpace);
	private long downloadingNowSize;

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

	public CachedFsClient with(MemSize cacheSizeLimit) {
		this.cacheSizeLimit = checkNotNull(cacheSizeLimit);
		return this;
	}

	public CachedFsClient with(CurrentTimeProvider timeProvider) {
		this.timeProvider = checkNotNull(timeProvider);
		return this;
	}
	// endregion

	public Stage<Void> setCacheSizeLimit(MemSize cacheSizeLimit) {
		this.cacheSizeLimit = checkNotNull(cacheSizeLimit);
		return ensureSpace();
	}

	public MemSize getCacheSizeLimit() {
		return this.cacheSizeLimit;
	}

	public Stage<MemSize> getTotalCacheSize() {
		return cacheClient.list()
				.thenCompose(list -> Stage.of(MemSize.of(list.stream().mapToLong(FileMetadata::getSize).sum())));
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Stage<Void> start() {
		checkState(cacheSizeLimit != null, "Cannot start cached client without specifying cache size limit");
		return ensureSpace();
	}

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		return mainClient.upload(filename, offset);
	}

	/**
	 * Tries to download file either from cache (if present) or from server.
	 *
	 * @param filename name of the file to be downloaded
	 * @param offset   from which byte to download the file
	 * @param length   how much bytes of the file do download
	 * @return stage for stream producer of byte buffers
	 */
	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		checkNotNull(filename, "fileName");
		checkArgument(offset >= 0, "Data offset must be greater than or equal to zero");
		checkArgument(length >= -1, "Data length must be either -1 or greater than or equal to zero");

		return cacheClient.getMetadata(filename)
				.thenCompose(cacheMetadata -> {
					if (cacheMetadata == null) {
						if (offset != 0) {
							return mainClient.download(filename, offset, length);
						}
						return mainClient.getMetadata(filename)
								.thenCompose(mainMetadata -> {
									if (mainMetadata == null) {
										return Stage.ofException(new RemoteFsException("File not found: " + filename));
									}
									return downloadToCache(filename, offset, length, 0);
								});
					}
					long sizeInCache = cacheMetadata.getSize();

					if (length != -1 && sizeInCache >= offset + length) {
						return cacheClient.download(filename, offset, length)
								.whenComplete((val, err) -> updateCacheStats(filename));
					}

					if (offset > sizeInCache) {
						return mainClient.download(filename, offset, length);
					}

					return mainClient.getMetadata(filename)
							.thenCompose(mainMetadata -> {
								if (mainMetadata == null) {
									return cacheClient.download(filename, offset, length)
											.whenComplete(($, e) -> updateCacheStats(filename));
								}

								long sizeInMain = mainMetadata.getSize();

								if (sizeInCache >= sizeInMain) {
									return cacheClient.download(filename, offset, length)
											.whenComplete((val, err) -> updateCacheStats(filename));
								}

								if ((length != -1) && (sizeInMain < (offset + length))) {
									String repr = filename + "(size=" + sizeInMain + (offset != 0 ? ", offset=" + offset : "") + ", length=" + length;
									return Stage.ofException(new RemoteFsException("Boundaries exceed file size: " + repr));
								}

								return downloadToCache(filename, offset, length, sizeInCache);
							});

				});
	}

	private Stage<SerialSupplier<ByteBuf>> downloadToCache(String fileName, long offset, long length, long sizeInCache) {
		long size = length == -1 ? length : length + offset - sizeInCache;
		return mainClient.download(fileName, sizeInCache, size)
				.thenApply(supplier -> {
					if (downloadingNowSize + size > cacheSizeLimit.toLong()) {
						return supplier;
					}

					SerialSplitter<ByteBuf> splitter = SerialSplitter.<ByteBuf>create()
							.withInput(supplier.transform(ByteBuf::slice));

					SerialSupplier<ByteBuf> output = splitter.addOutput().getOutputSupplier().transform(b -> {
						ByteBuf slice = b.slice();
						b.recycle();
						return slice;
					});

					long cacheOffset = sizeInCache == 0 ? -1 : sizeInCache;
					downloadingNowSize += size;

					MaterializedStage<Void> cacheAppendProcess = splitter.addOutput()
							.getOutputSupplier()
							.transform(b -> {
								ByteBuf slice = b.slice();
								b.recycle();
								return slice;
							})
							.streamTo(cacheClient.uploadSerial(fileName, cacheOffset))
//							.thenCompose($ -> cacheClient.list())
							.thenCompose($ -> updateCacheStats(fileName))
							.thenCompose($ -> ensureSpace())
							.whenResult($ -> downloadingNowSize -= size)
							.materialize();

					splitter.start();

					return (sizeInCache == 0 ? output : SerialSuppliers.concat(cacheClient.downloadSerial(fileName, offset, sizeInCache), output))
							.withEndOfStream(stage -> stage.thenCompose($ -> cacheAppendProcess));
				});
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return mainClient.move(changes);
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return mainClient.copy(changes);
	}

	/**
	 * Lists files that are matched by glob. List is combined from cache folder files and files that are on server.
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return stage that is a union of the most actual files from cache folder and server
	 */
	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return Stages.toList(cacheClient.list(glob), mainClient.list(glob))
				.thenApply(lists -> lists.stream().flatMap(List::stream))
				.thenApply(list -> {
					Map<String, FileMetadata> mapOfMeta = new HashMap<>();
					list.forEach(metaNew ->
							mapOfMeta.compute(metaNew.getName(), (s, metaExisting) ->
									FileMetadata.getMoreCompleteFile(metaExisting, metaNew)));
					return new ArrayList<>(mapOfMeta.values());
				});
	}

	/**
	 * Deletes file both on server and in cache folder
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return stage of {@link Void} that represents succesfull deletion
	 */
	@Override
	public Stage<Void> delete(String glob) {
		return
				Stages.all(
						cacheClient.list(glob)
								.whenResult(listOfMeta -> listOfMeta.forEach(meta -> cacheStats.remove(meta.getName())))
								.thenApply(listOfMeta -> listOfMeta.stream().mapToLong(FileMetadata::getSize).sum())
								.whenResult($ -> cacheClient.delete(glob)),
						mainClient.delete(glob)
				);
	}

	@Override
	public Stage<Void> stop() {
		return ensureSpace();
	}

	private Stage<Void> updateCacheStats(String fileName) {
		return Stage.
				of(cacheStats
						.computeIfPresent(fileName, (s, cacheStat) -> {
							cacheStat.numberOfHits++;
							cacheStat.lastHitTimestamp = timeProvider.currentTimeMillis();
							return cacheStat;
						}))
				.whenResult($ -> cacheStats
						.computeIfAbsent(fileName, s -> new CacheStat(1, timeProvider.currentTimeMillis())))
				.toVoid();
	}

	private Stage<Void> ensureSpace() {
		return ensureSpace.get();
	}

	private Stage<Void> doEnsureSpace() {
		long[] sizeAccum = {0};
		return cacheClient.list()
				.thenApply(list -> list
						.stream()
						.map(metadata -> {
							CacheStat cacheStat = cacheStats.get(metadata.getName());
							return cacheStat == null ?
									new FullCacheStat(metadata, 0, 0) :
									new FullCacheStat(metadata, cacheStat.numberOfHits, cacheStat.lastHitTimestamp);
						})
						.sorted(comparator.reversed())
						.filter(fullCacheStat -> {
							sizeAccum[0] += fullCacheStat.getFileMetadata().getSize();
							return sizeAccum[0] > cacheSizeLimit.toLong();
						}))
				.thenCompose(filesToDelete -> Stages.all(filesToDelete
						.map(fullCacheStat -> cacheClient
								.delete(fullCacheStat.getFileMetadata().getName())
								.whenResult($ -> cacheStats.remove(fullCacheStat.fileMetadata.getName()))))
				);
	}

	private final class CacheStat {
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
	public final class FullCacheStat {
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
