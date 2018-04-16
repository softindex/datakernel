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
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.file.AsyncFile;
import io.datakernel.jmx.*;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerModifier;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.*;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.Initializable;
import io.datakernel.util.MemSize;
import io.datakernel.util.ReflectionUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.datakernel.aggregation.AggregationUtils.createBufferSerializer;
import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;
import static java.nio.file.StandardOpenOption.READ;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Stores aggregation chunks in local file system.
 */
public class LocalFsChunkStorage implements AggregationChunkStorage, EventloopService, Initializable<LocalFsChunkStorage>, EventloopJmxMBeanEx {
	private final Logger logger = getLogger(this.getClass());
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);
	public static final String DEFAULT_BACKUP_FOLDER_NAME = "backups";
	public static final String LOG = ".log";
	public static final String TEMP_LOG = ".temp";

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final IdGenerator<Long> idGenerator;

	private final Path dir;
	private Path backupPath;

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;

	private final StageStats stageIdGenerator = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR1 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR2 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenR3 = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageOpenW = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageFinishChunks = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageList = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageBackup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageCleanup = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

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
	private long cleanupSkipTimeMin;
	private long cleanupSkipTimeMax;
	private int cleanupSkippedFiles;
	private int cleanupSkippedFilesTotal;

	private int finishChunks;

	/**
	 * Constructs an aggregation storage, that runs in the specified event loop, performs blocking IO in the given executor,
	 * serializes records according to specified aggregation structure and stores data in the given directory.
	 *
	 * @param eventloop       event loop, in which aggregation storage is to run
	 * @param executorService executor, where blocking IO operations are to be run
	 * @param idGenerator
	 * @param dir             directory where data is saved
	 */
	private LocalFsChunkStorage(Eventloop eventloop, ExecutorService executorService, IdGenerator<Long> idGenerator, Path dir, Path backUpPath) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.dir = dir;
		this.idGenerator = idGenerator;
		this.backupPath = backUpPath;
	}

	public static LocalFsChunkStorage create(Eventloop eventloop, ExecutorService executorService, IdGenerator<Long> idGenerator, Path dir) {
		return new LocalFsChunkStorage(eventloop, executorService, idGenerator, dir, dir.resolve(DEFAULT_BACKUP_FOLDER_NAME + "/"));
	}

	public LocalFsChunkStorage withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public LocalFsChunkStorage withBackupPath(Path backupPath) {
		this.backupPath = backupPath;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Stage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields,
	                                                         Class<T> recordClass, long chunkId,
	                                                         DefiningClassLoader classLoader) {
		return Stages.first(
				() -> AsyncFile.openAsync(executorService, dir.resolve(chunkId + LOG), new OpenOption[]{READ}).whenComplete(stageOpenR1.recordStats()),
				() -> AsyncFile.openAsync(executorService, dir.resolve(chunkId + TEMP_LOG), new OpenOption[]{READ}).whenComplete(stageOpenR2.recordStats()),
				() -> AsyncFile.openAsync(executorService, dir.resolve(chunkId + LOG), new OpenOption[]{READ}).whenComplete(stageOpenR3.recordStats()))
				.thenApply(file -> StreamFileReader.readFile(file).withBufferSize(bufferSize)
						.with(readFile)
						.with(StreamLZ4Decompressor.create())
						.with(readDecompress)
						.with(StreamBinaryDeserializer.create(
								createBufferSerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader)))
						.with((StreamProducerModifier<T, T>) (detailed ? readDeserializeDetailed : readDeserialize))
						.withEndOfStreamAsResult()
						.withLateBinding());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Stage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields,
	                                                          Class<T> recordClass, long id,
	                                                          DefiningClassLoader classLoader) {
		return AsyncFile.openAsync(executorService, dir.resolve(id + TEMP_LOG), StreamFileWriter.CREATE_OPTIONS)
				.whenComplete(stageOpenW.recordStats())
				.thenApply(file -> StreamTransformer.<T>idenity()
						.with((StreamProducerModifier<T, T>) (detailed ? writeSerializeDetailed : writeSerialize))
						.with(StreamBinarySerializer.create(
								createBufferSerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader))
								.withInitialBufferSize(bufferSize))
						.with(writeCompress)
						.with(StreamLZ4Compressor.fastCompressor())
						.with(writeChunker)
						.with(StreamByteChunker.create(
								bufferSize.map(bytes -> bytes / 2),
								bufferSize.map(bytes -> bytes * 2)))
						.with(writeFile)
						.applyTo(StreamFileWriter.create(file).withForceOnClose(true).withFlushAsResult()));
	}

	@Override
	public Stage<Long> createId() {
		return idGenerator.createId().whenComplete(stageIdGenerator.recordStats());
	}

	@Override
	public Stage<Void> finish(Set<Long> chunkIds) {
		finishChunks = chunkIds.size();
		return Stage.ofCallable(executorService, () -> {
			for (Long chunkId : chunkIds) {
				Path tempLog = dir.resolve(chunkId + TEMP_LOG);
				Path log = dir.resolve(chunkId + LOG);
				Files.setLastModifiedTime(tempLog, FileTime.fromMillis(System.currentTimeMillis()));
				Files.move(tempLog, log, StandardCopyOption.ATOMIC_MOVE);
			}
			return (Void) null;
		}).whenComplete(stageFinishChunks.recordStats());
	}

	public Stage<Void> backup(String backupId, Set<Long> chunkIds) {
		return Stage.ofCallable(executorService, () -> {
			Path tempBackupDir = backupPath.resolve(backupId + "_tmp/");
			Files.createDirectories(tempBackupDir);
			for (long chunkId : chunkIds) {
				Path target = dir.resolve(chunkId + LOG).toAbsolutePath();
				Path link = tempBackupDir.resolve(chunkId + LOG).toAbsolutePath();
				Files.createLink(link, target);
			}

			Path backupDir = backupPath.resolve(backupId + "/");
			Files.move(tempBackupDir, backupDir, StandardCopyOption.ATOMIC_MOVE);
			return (Void) null;
		}).whenComplete(stageBackup.recordStats());
	}

	public Stage<Void> cleanup(Set<Long> saveChunks) {
		return cleanupBeforeTimestamp(saveChunks, -1);
	}

	public Stage<Void> cleanupBeforeTimestamp(Set<Long> preserveChunks, long timestamp) {
		return Stage.ofCallable(executorService, () -> {
			logger.trace("Cleanup before timestamp, save chunks size: {}, timestamp {}", preserveChunks.size(), timestamp);
			int skipped = 0;
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
				List<Path> filesToDelete = new ArrayList<>();

				for (Path file : stream) {
					if (!file.toString().endsWith(LOG)) {
						continue;
					}
					long id;
					try {
						String filename = file.getFileName().toString();
						id = Long.parseLong((filename.substring(0, filename.length() - LOG.length())));
					} catch (NumberFormatException e) {
						cleanupWarnings.recordException(e);
						logger.warn("Invalid chunk filename: " + file);
						continue;
					}
					if (preserveChunks.contains(id)) continue;
					FileTime lastModifiedTime = Files.getLastModifiedTime(file);
					if (timestamp != -1 && lastModifiedTime.toMillis() > timestamp) {
						long difference = lastModifiedTime.toMillis() - timestamp;
						assert difference > 0;
						if (cleanupSkipTimeMin == 0 || difference < cleanupSkipTimeMin) {
							cleanupSkipTimeMin = difference;
						}
						if (cleanupSkipTimeMax == 0 || difference > cleanupSkipTimeMax) {
							cleanupSkipTimeMax = difference;
						}
						logger.warn("File {} timestamp {} > {}",
								file, lastModifiedTime.toMillis(), timestamp);
						skipped++;
						continue;
					}

					filesToDelete.add(file);
				}

				for (Path file : filesToDelete) {
					try {
						if (logger.isTraceEnabled()) {
							FileTime lastModifiedTime = Files.getLastModifiedTime(file);
							logger.trace("Delete file: {} with last modifiedTime: {}({} millis)", file,
									lastModifiedTime, lastModifiedTime.toMillis());
						}
						Files.delete(file);
					} catch (IOException e) {
						cleanupWarnings.recordException(e);
						logger.warn("Could not delete file: " + file);
					}
				}

				cleanupPreservedFiles = preserveChunks.size();
				cleanupDeletedFiles = filesToDelete.size();
				cleanupDeletedFilesTotal += filesToDelete.size();
				cleanupSkippedFiles = skipped;
				cleanupSkippedFilesTotal += skipped;
			}
			return (Void) null;
		}).whenComplete(stageCleanup.recordStats());
	}

	public Stage<Set<Long>> list(Predicate<String> filter, Predicate<Long> lastModified) {
		return Stage.ofCallable(executorService, () -> {
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
				return StreamSupport.stream(stream.spliterator(), false)
						.filter(file -> lastModifiedFilter(lastModified, file))
						.map(file -> file.getFileName().toString())
						.filter(name -> name.endsWith(LOG) && filter.test(name))
						.map(name -> Long.parseLong(name.substring(0, name.length() - LOG.length())))
						.collect(Collectors.toSet());
			}
		}).whenComplete(stageList.recordStats());
	}

	private boolean lastModifiedFilter(Predicate<Long> lastModified, Path file) {
		try {
			return lastModified.test(Files.getLastModifiedTime(file).toMillis());
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Stage<Void> start() {
		return Stage.ofCallable(executorService, () -> Files.createDirectories(dir)).toVoid();
	}

	@Override
	public Stage<Void> stop() {
		return Stage.of(null);
	}

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
	public StageStats getStageOpenR1() {
		return stageOpenR1;
	}

	@JmxAttribute
	public StageStats getStageOpenR2() {
		return stageOpenR2;
	}

	@JmxAttribute
	public StageStats getStageOpenR3() {
		return stageOpenR3;
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
	public long getCleanupSkipTimeMin() {
		return cleanupSkipTimeMin;
	}

	@JmxAttribute
	public long getCleanupSkipTimeMax() {
		return cleanupSkipTimeMax;
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
		cleanupSkipTimeMin = 0;
		cleanupSkipTimeMax = 0;
		ReflectionUtils.resetStats(this);
	}
}
