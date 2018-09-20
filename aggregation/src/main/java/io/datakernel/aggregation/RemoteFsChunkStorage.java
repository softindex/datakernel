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

package io.datakernel.aggregation;

import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.*;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.processor.*;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.Initializable;
import io.datakernel.util.MemSize;
import io.datakernel.util.ReflectionUtils;
import org.slf4j.Logger;

import java.io.File;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.datakernel.aggregation.AggregationUtils.createBufferSerializer;
import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.toLimitedString;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

public final class RemoteFsChunkStorage<C> implements AggregationChunkStorage<C>, EventloopService, Initializable<RemoteFsChunkStorage<C>>, EventloopJmxMBeanEx {
	private static final Logger logger = getLogger(RemoteFsChunkStorage.class);
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);
	public static final String DEFAULT_BACKUP_FOLDER_NAME = "backups";
	public static final String LOG = ".log";
	public static final String TEMP_LOG = ".temp";

	private final Eventloop eventloop;
	private final ChunkIdScheme<C> chunkIdScheme;
	private final IdGenerator<C> idGenerator;

	private final FsClient client;
	private String backupDir = DEFAULT_BACKUP_FOLDER_NAME;

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;

	private final ValueStats chunksCount = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageIdGenerator = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenW = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageFinishChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageList = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageBackup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanupCheckRequiredChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	private boolean detailed;

	private final StreamStatsDetailed<ByteBuf> readFile = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> readDecompress = StreamStats.detailed(forByteBufs());
	private final StreamStatsBasic<?> readDeserialize = StreamStats.basic();
	private final StreamStatsDetailed<?> readDeserializeDetailed = StreamStats.detailed();

	private final StreamStatsBasic<?> writeSerialize = StreamStats.basic();
	private final StreamStatsDetailed<?> writeSerializeDetailed = StreamStats.detailed();
	private final StreamStatsDetailed<ByteBuf> writeCompress = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> writeChunker = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> writeFile = StreamStats.detailed(forByteBufs());

	private final ExceptionStats cleanupWarnings = ExceptionStats.create();
	private int cleanupPreservedFiles;
	private int cleanupDeletedFiles;
	private int cleanupDeletedFilesTotal;
	private int cleanupSkippedFiles;
	private int cleanupSkippedFilesTotal;

	private int finishChunks;

	private RemoteFsChunkStorage(Eventloop eventloop, ChunkIdScheme<C> chunkIdScheme, IdGenerator<C> idGenerator, FsClient client) {
		this.eventloop = eventloop;
		this.chunkIdScheme = chunkIdScheme;
		this.idGenerator = idGenerator;
		this.client = client;
	}

	public static <C> RemoteFsChunkStorage<C> create(Eventloop eventloop,
			ChunkIdScheme<C> chunkIdScheme,
			IdGenerator<C> idGenerator, FsClient client) {
		return new RemoteFsChunkStorage<>(eventloop, chunkIdScheme, idGenerator, client);
	}

	public RemoteFsChunkStorage<C> withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public RemoteFsChunkStorage withBackupPath(String backupDir) {
		this.backupDir = backupDir;
		return this;
	}

	private String getPath(C chunkId) {
		return toFileName(chunkId) + LOG;
	}

	private String getTempPath(C chunkId) {
		return toFileName(chunkId) + TEMP_LOG;
	}

	private String toFileName(C chunkId) {
		return chunkIdScheme.toFileName(chunkId);
	}

	private C fromFileName(String fileName) {
		return chunkIdScheme.fromFileName(fileName);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Stage<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId,
			DefiningClassLoader classLoader) {
		return client.download(getPath(chunkId))
				.whenComplete(stageOpenR.recordStats())
				.thenApply(supplier -> supplier
						.apply(readFile)
						.apply(SerialLZ4Decompressor.create())
						.apply(readDecompress)
						.apply(SerialBinaryDeserializer.create(
								createBufferSerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader)))
						.apply((StreamStats<T>) (detailed ? readDeserializeDetailed : readDeserialize))
						.withLateBinding());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Stage<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId,
			DefiningClassLoader classLoader) {
		return client.upload(getTempPath(chunkId))
				.whenComplete(stageOpenW.recordStats())
				.thenApply(consumer -> StreamConsumer.ofSupplier(
						supplier -> supplier
								.apply((StreamStats<T>) (detailed ? writeSerializeDetailed : writeSerialize))
								.apply(SerialBinarySerializer.create(
										createBufferSerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader))
										.withInitialBufferSize(bufferSize))
								.apply(writeCompress)
								.apply(SerialLZ4Compressor.createFastCompressor())
								.apply(writeChunker)
								.apply(SerialByteChunker.create(
										bufferSize.map(bytes -> bytes / 2),
										bufferSize.map(bytes -> bytes * 2)))
								.apply(writeFile)
								.streamTo(consumer)));
	}

	@Override
	public Stage<Void> finish(Set<C> chunkIds) {
		finishChunks = chunkIds.size();
		return client.strictMove(chunkIds.stream().collect(toMap(this::getTempPath, this::getPath)))
				.whenComplete(stageFinishChunks.recordStats());
	}

	@Override
	public Stage<C> createId() {
		return idGenerator.createId().whenComplete(stageIdGenerator.recordStats());
	}

	public Stage<Void> backup(String backupId, Set<C> chunkIds) {
		String tempBackupDir = backupDir + File.separator + backupId + "_tmp";

		return Stages.all(chunkIds.stream().map(chunkId -> client.copy(chunkId + LOG, tempBackupDir + File.separator + chunkId + LOG)))
				.thenCompose($ -> client.move(tempBackupDir, backupDir + File.separator + backupId))
				.whenComplete(stageBackup.recordStats());
	}

	public Stage<Void> cleanup(Set<C> saveChunks) {
		return cleanup(saveChunks, null);
	}

	public Stage<Void> cleanup(Set<C> preserveChunks, @Nullable Instant instant) {
		long timestamp = instant != null ? instant.toEpochMilli() : -1;
		int[] skipped = {0};
		int[] deleted = {0};
		return client.listLocal()
				.thenCompose(list -> Stages.all(list.stream()
						.filter(file -> {
							if (!file.getName().endsWith(LOG)) {
								return false;
							}
							C id;
							try {
								String filename = file.getName();
								id = fromFileName(filename.substring(0, filename.length() - LOG.length()));
							} catch (NumberFormatException e) {
								cleanupWarnings.recordException(e);
								logger.warn("Invalid chunk filename: " + file);
								return false;
							}
							if (preserveChunks.contains(id)) {
								return false;
							}
							long fileTimestamp = file.getTimestamp();
							if (timestamp == -1 || fileTimestamp <= timestamp) {
								return true;
							}
							long difference = fileTimestamp - timestamp;
							assert difference > 0;
							logger.trace("File {} timestamp {} > {}", file, fileTimestamp, timestamp);
							skipped[0]++;
							return false;
						})
						.map(file -> {
							if (logger.isTraceEnabled()) {
								FileTime lastModifiedTime = FileTime.fromMillis(file.getTimestamp());
								logger.trace("Delete file: {} with last modifiedTime: {}({} millis)", file.getName(),
										lastModifiedTime, lastModifiedTime.toMillis());
							}
							deleted[0]++;
							return client.delete(file.getName());
						}))
						.thenRun(() -> {
							cleanupPreservedFiles = preserveChunks.size();
							cleanupDeletedFiles = deleted[0];
							cleanupDeletedFilesTotal += deleted[0];
							cleanupSkippedFiles = skipped[0];
							cleanupSkippedFilesTotal += skipped[0];
						}))
				.whenComplete(stageCleanup.recordStats());
	}

	public Stage<Set<Long>> list(Predicate<String> filter, Predicate<Long> lastModified) {
		return client.list()
				.thenApply(list ->
						list.stream()
								.filter(file -> lastModified.test(file.getTimestamp()))
								.map(FileMetadata::getName)
								.filter(name -> name.endsWith(LOG) && filter.test(name))
								.map(name -> Long.parseLong(name.substring(0, name.length() - LOG.length())))
								.collect(Collectors.toSet()))
				.whenComplete(stageList.recordStats());
	}

	public Stage<Void> checkRequiredChunks(Set<C> requiredChunks) {
		return list(s -> true, timestamp -> true)
				.whenResult(actualChunks -> chunksCount.recordValue(actualChunks.size()))
				.thenCompose(actualChunks -> actualChunks.containsAll(requiredChunks) ?
						Stage.of((Void) null) :
						Stage.ofException(new IllegalStateException("Missed chunks from storage: " +
								toLimitedString(difference(requiredChunks, actualChunks), 100))))
				.whenComplete(stageCleanupCheckRequiredChunks.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), toLimitedString(requiredChunks, 6)));
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Stage<Void> start() {
		return client.ping();
	}

	@Override
	public Stage<Void> stop() {
		return Stage.complete();
	}

	// region JMX

	@JmxAttribute
	public StageStats getStageIdGenerator() {
		return stageIdGenerator;
	}

	@JmxAttribute
	public StageStats getStageFinishChunks() {
		return stageFinishChunks;
	}

	@JmxAttribute
	public StageStats getStageBackup() {
		return stageBackup;
	}

	@JmxAttribute
	public StageStats getStageCleanup() {
		return stageCleanup;
	}

	@JmxAttribute
	public StageStats getStageList() {
		return stageList;
	}

	@JmxAttribute
	public StageStats getStageOpenR() {
		return stageOpenR;
	}

	@JmxAttribute
	public StageStats getStageOpenW() {
		return stageOpenW;
	}

	@JmxAttribute
	public StreamStatsDetailed getReadFile() {
		return readFile;
	}

	@JmxAttribute
	public StreamStatsDetailed getReadDecompress() {
		return readDecompress;
	}

	@JmxAttribute
	public StreamStatsBasic getReadDeserialize() {
		return readDeserialize;
	}

	@JmxAttribute
	public StreamStatsDetailed getReadDeserializeDetailed() {
		return readDeserializeDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getWriteSerialize() {
		return writeSerialize;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteSerializeDetailed() {
		return writeSerializeDetailed;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteCompress() {
		return writeCompress;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteChunker() {
		return writeChunker;
	}

	@JmxAttribute
	public StreamStatsDetailed getWriteFile() {
		return writeFile;
	}

	@JmxAttribute
	public int getFinishChunks() {
		return finishChunks;
	}

	@JmxAttribute
	public ExceptionStats getCleanupWarnings() {
		return cleanupWarnings;
	}

	@JmxAttribute
	public int getCleanupPreservedFiles() {
		return cleanupPreservedFiles;
	}

	@JmxAttribute
	public int getCleanupDeletedFiles() {
		return cleanupDeletedFiles;
	}

	@JmxAttribute
	public int getCleanupDeletedFilesTotal() {
		return cleanupDeletedFilesTotal;
	}

	@JmxAttribute
	public int getCleanupSkippedFiles() {
		return cleanupSkippedFiles;
	}

	@JmxAttribute
	public int getCleanupSkippedFilesTotal() {
		return cleanupSkippedFilesTotal;
	}

	@JmxAttribute
	public ValueStats getChunksCount() {
		return chunksCount;
	}

	@JmxAttribute
	public StageStats getStageCleanupCheckRequiredChunks() {
		return stageCleanupCheckRequiredChunks;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailed = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailed = false;
	}

	@Override
	public void resetStats() {
		cleanupPreservedFiles = 0;
		cleanupDeletedFiles = 0;
		cleanupDeletedFilesTotal = 0;
		cleanupSkippedFiles = 0;
		cleanupSkippedFilesTotal = 0;
		ReflectionUtils.resetStats(this);
	}
	// endregion
}
