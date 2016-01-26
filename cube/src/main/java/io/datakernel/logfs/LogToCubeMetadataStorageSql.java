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

package io.datakernel.logfs;

import com.google.common.collect.Multimap;
import io.datakernel.aggregation_db.*;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.CubeMetadataStorageSql;
import io.datakernel.cube.sql.tables.records.AggregationDbLogRecord;
import io.datakernel.eventloop.Eventloop;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.TransactionalRunnable;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.callConcurrently;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static io.datakernel.cube.sql.tables.AggregationDbLog.AGGREGATION_DB_LOG;

/**
 * Stores cube and logs metadata in relational database.
 */
public final class LogToCubeMetadataStorageSql implements LogToCubeMetadataStorage {
	private static final Logger logger = LoggerFactory.getLogger(LogToCubeMetadataStorageSql.class);

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;
	private final CubeMetadataStorageSql cubeMetadataStorage;
	private final AggregationMetadataStorageSql aggregationMetadataStorage;

	/**
	 * Constructs a metadata storage for cube metadata and logs, that runs in the specified event loop,
	 * performs SQL queries in the given executor, connects to RDBMS using the specified configuration,
	 * and uses the given cube metadata storage for some of the operations.
	 *
	 * @param eventloop                  event loop, in which metadata storage is to run
	 * @param executor                   executor, where SQL queries are to be run
	 * @param jooqConfiguration          database connection configuration
	 * @param cubeMetadataStorage        cube metadata storage
	 * @param aggregationMetadataStorage aggregation metadata storage
	 */
	public LogToCubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor,
	                                   Configuration jooqConfiguration, CubeMetadataStorageSql cubeMetadataStorage,
	                                   AggregationMetadataStorageSql aggregationMetadataStorage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
		this.cubeMetadataStorage = cubeMetadataStorage;
		this.aggregationMetadataStorage = aggregationMetadataStorage;
	}

	void truncateTables(DSLContext jooq) {
		aggregationMetadataStorage.truncateTables(jooq);
		jooq.truncate(AGGREGATION_DB_LOG).execute();
	}

	/**
	 * Truncates tables that are used for storing cube and log metadata.
	 */
	public void truncateTables() {
		truncateTables(DSL.using(jooqConfiguration));
	}

	private Map<String, LogPosition> loadLogPositions(DSLContext jooq,
	                                                  String log, List<String> partitions) {
		Result<AggregationDbLogRecord> logRecords = jooq.selectFrom(AGGREGATION_DB_LOG)
				.where(AGGREGATION_DB_LOG.LOG.eq(log))
				.and(AGGREGATION_DB_LOG.PARTITION.in(partitions))
				.fetch();

		Map<String, LogPosition> logPositionMap = new LinkedHashMap<>();

		for (String partition : partitions) {
			logPositionMap.put(partition, new LogPosition());
		}

		for (AggregationDbLogRecord logRecord : logRecords) {
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

	@Override
	public void saveCommit(final String log, final String processId,
	                       final Map<String, LogPosition> oldPositions,
	                       final Map<String, LogPosition> newPositions,
	                       final Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks,
	                       CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				saveCommit(log, processId, oldPositions, newPositions, newChunks);
			}
		}, callback);
	}

	private void saveCommit(final String log, final String processId, Map<String, LogPosition> oldPositions,
	                        final Map<String, LogPosition> newPositions,
	                        final Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks) {
		final Connection connection = jooqConfiguration.connectionProvider().acquire();
		final Configuration configurationWithConnection = jooqConfiguration.derive(connection);
		DSLContext jooq = DSL.using(configurationWithConnection);
		try {
			aggregationMetadataStorage.getCubeLock(jooq);
			jooq.transaction(new TransactionalRunnable() {
				@Override
				public void run(Configuration configuration) throws Exception {
					DSLContext jooq = DSL.using(configurationWithConnection);

					for (String partition : newPositions.keySet()) {
						LogPosition logPosition = newPositions.get(partition);
						logger.info("Finished reading logs at position {}", logPosition);

						if (logPosition.getLogFile() == null)
							continue;

						jooq.insertInto(AGGREGATION_DB_LOG)
								.set(new AggregationDbLogRecord(
										log,
										partition,
										logPosition.getLogFile().getName(),
										logPosition.getLogFile().getN(),
										logPosition.getPosition()))
								.onDuplicateKeyUpdate()
								.set(AGGREGATION_DB_LOG.FILE, logPosition.getLogFile().getName())
								.set(AGGREGATION_DB_LOG.FILE_INDEX, logPosition.getLogFile().getN())
								.set(AGGREGATION_DB_LOG.POSITION, logPosition.getPosition())
								.execute();
					}

					if (!newChunks.isEmpty())
						aggregationMetadataStorage.saveNewChunks(jooq, processId, newChunks);
				}
			});
		} finally {
			aggregationMetadataStorage.releaseCubeLock(jooq);
			jooqConfiguration.connectionProvider().release(connection);
		}
	}

	@Override
	public void loadAggregations(AggregationStructure structure, ResultCallback<List<AggregationMetadata>> callback) {
		cubeMetadataStorage.loadAggregations(structure, callback);
	}

	@Override
	public void saveAggregations(AggregationStructure structure, Collection<Aggregation> aggregations,
	                             CompletionCallback callback) {
		cubeMetadataStorage.saveAggregations(structure, aggregations, callback);
	}
}
