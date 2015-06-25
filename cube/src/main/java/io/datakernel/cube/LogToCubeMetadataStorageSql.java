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

package io.datakernel.cube;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.sql.tables.records.AggregationLogRecord;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.LogFile;
import io.datakernel.logfs.LogPosition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.TransactionalRunnable;
import org.jooq.impl.DSL;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.callConcurrently;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static io.datakernel.cube.sql.tables.AggregationLog.AGGREGATION_LOG;

/**
 * Stores cube and logs metadata in relational database.
 */
public final class LogToCubeMetadataStorageSql implements LogToCubeMetadataStorage {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;
	private final CubeMetadataStorageSql cubeMetadataStorage;

	/**
	 * Constructs a metadata storage for cube metadata and logs, that runs in the specified event loop,
	 * performs SQL queries in the given executor, connects to RDBMS using the specified configuration,
	 * and uses the given cube metadata storage for some of the operations.
	 *
	 * @param eventloop           event loop, in which metadata storage is to run
	 * @param executor            executor, where SQL queries are to be run
	 * @param jooqConfiguration   database connection configuration
	 * @param cubeMetadataStorage cube metadata storage
	 */
	public LogToCubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor,
	                                   Configuration jooqConfiguration, CubeMetadataStorageSql cubeMetadataStorage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
		this.cubeMetadataStorage = cubeMetadataStorage;
	}

	void truncateTables(DSLContext jooq) {
		cubeMetadataStorage.truncateTables(jooq);
		jooq.truncate(AGGREGATION_LOG).execute();
	}

	/**
	 * Truncates tables that are used for storing cube and log metadata.
	 */
	public void truncateTables() {
		truncateTables(DSL.using(jooqConfiguration));
	}

	private Map<String, LogPosition> loadLogPositions(DSLContext jooq,
	                                                  String log, List<String> partitions) {
		Result<AggregationLogRecord> logRecords = jooq.selectFrom(AGGREGATION_LOG)
				.where(AGGREGATION_LOG.LOG.eq(log))
				.and(AGGREGATION_LOG.PARTITION.in(partitions))
//			.forUpdate()
				.fetch();

		Map<String, LogPosition> logPositionMap = new LinkedHashMap<>();
		for (String partition : partitions) {
			logPositionMap.put(partition, new LogPosition());
		}
		for (AggregationLogRecord logRecord : logRecords) {
			logPositionMap.put(logRecord.getPartition(),
					new LogPosition(
							new LogFile(logRecord.getFile(), logRecord.getFileIndex()),
							logRecord.getPosition()));
		}
		return logPositionMap;
	}

	@Override
	public void loadLogPositions(final String log, final List<String> partitions, final ResultCallback<Map<String, LogPosition>> callback) {
		callConcurrently(eventloop, executor, false, new Callable<Map<String, LogPosition>>() {
			@Override
			public Map<String, LogPosition> call() throws Exception {
				return loadLogPositions(DSL.using(jooqConfiguration), log, partitions);
			}
		}, callback);
	}

	private void processLog_commit(DSLContext jooq,
	                               final Cube cube, final String log, Map<String, LogPosition> oldPositions, final Map<String, LogPosition> newPositions, final Multimap<Aggregation, AggregationChunk.NewChunk> newChunks) {
		jooq.transaction(new TransactionalRunnable() {
			@Override
			public void run(Configuration configuration) throws Exception {
				DSLContext jooq = DSL.using(configuration);

				for (String partition : newPositions.keySet()) {
					LogPosition logPosition = newPositions.get(partition);
					jooq.insertInto(AGGREGATION_LOG)
							.set(new AggregationLogRecord(
									log,
									partition,
									logPosition.getLogFile().getName(),
									logPosition.getLogFile().getN(),
									logPosition.getPosition()))
							.onDuplicateKeyUpdate()
							.set(AGGREGATION_LOG.FILE, logPosition.getLogFile().getName())
							.set(AGGREGATION_LOG.FILE_INDEX, logPosition.getLogFile().getN())
							.set(AGGREGATION_LOG.POSITION, logPosition.getPosition())
							.execute();
				}

				cubeMetadataStorage.saveChunks(jooq, newChunks);
			}
		});

		cubeMetadataStorage.loadAggregations(jooq, cube);
		cube.setLastRevisionId(cubeMetadataStorage.loadChunks(jooq, cube, cube.getLastRevisionId(), Integer.MAX_VALUE));
	}

	@Override
	public void commit(final Cube cube, final String log,
	                   final Map<String, LogPosition> oldPositions,
	                   final Map<String, LogPosition> newPositions,
	                   final Multimap<Aggregation, AggregationChunk.NewChunk> newChunks,
	                   CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				processLog_commit(DSL.using(jooqConfiguration), cube, log, oldPositions, newPositions, newChunks);
			}
		}, callback);
	}

	@Override
	public void newChunkId(ResultCallback<Long> callback) {
		cubeMetadataStorage.newChunkId(callback);
	}

	@Override
	public long newChunkId() {
		return cubeMetadataStorage.newChunkId();
	}

	@Override
	public void loadAggregations(Cube cube, CompletionCallback callback) {
		cubeMetadataStorage.loadAggregations(cube, callback);
	}

	@Override
	public void saveAggregations(Cube cube, CompletionCallback callback) {
		cubeMetadataStorage.saveAggregations(cube, callback);
	}

	@Override
	public void saveChunks(Aggregation aggregation, List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
		cubeMetadataStorage.saveChunks(aggregation, newChunks, callback);
	}

	@Override
	public void loadChunks(Cube cube, int lastRevisionId, int maxRevisionId, ResultCallback<Integer> callback) {
		cubeMetadataStorage.loadChunks(cube, lastRevisionId, maxRevisionId, callback);
	}

	@Override
	public void reloadAllChunkConsolidations(Cube cube, CompletionCallback callback) {
		cubeMetadataStorage.reloadAllChunkConsolidations(cube, callback);
	}

	@Override
	public void saveConsolidatedChunks(Cube cube, Aggregation aggregation, List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
		cubeMetadataStorage.saveConsolidatedChunks(cube, aggregation, originalChunks, consolidatedChunks, callback);
	}

	@Override
	public void performConsolidation(Cube cube, Function<List<Long>, List<AggregationChunk>> chunkConsolidator, CompletionCallback callback) {
		cubeMetadataStorage.performConsolidation(cube, chunkConsolidator, callback);
	}

	@Override
	public void refreshNotConsolidatedChunks(Cube cube, CompletionCallback callback) {
		cubeMetadataStorage.refreshNotConsolidatedChunks(cube, callback);
	}

	@Override
	public void removeChunk(long chunkId, CompletionCallback callback) {
		cubeMetadataStorage.removeChunk(chunkId, callback);
	}
}
