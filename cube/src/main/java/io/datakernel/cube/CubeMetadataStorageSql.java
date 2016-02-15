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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.AggregationMetadataStorage.LoadedChunks;
import io.datakernel.aggregation_db.sql.tables.records.AggregationDbChunkRecord;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.aggregation_db.AggregationChunk.createChunk;
import static io.datakernel.aggregation_db.sql.tables.AggregationDbChunk.AGGREGATION_DB_CHUNK;
import static io.datakernel.aggregation_db.sql.tables.AggregationDbRevision.AGGREGATION_DB_REVISION;
import static io.datakernel.aggregation_db.util.JooqUtils.onDuplicateKeyUpdateValues;
import static io.datakernel.async.AsyncCallbacks.callConcurrently;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static org.jooq.impl.DSL.currentTimestamp;

public class CubeMetadataStorageSql implements CubeMetadataStorage {
	private static final Logger logger = LoggerFactory.getLogger(CubeMetadataStorageSql.class);

	private static final String LOCK_NAME = "cube_lock";
	private static final int DEFAULT_LOCK_TIMEOUT_SECONDS = 180;
	private static final int MAX_KEYS = 40;
	private static final Joiner JOINER = Joiner.on(' ');
	private static final Splitter SPLITTER = Splitter.on(' ').omitEmptyStrings();

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;
	private final int lockTimeoutSeconds;
	private final String processId;

	public CubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration, String processId) {
		this(eventloop, executor, jooqConfiguration, DEFAULT_LOCK_TIMEOUT_SECONDS, processId);
	}

	public CubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration,
	                              int lockTimeoutSeconds, String processId) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
		this.lockTimeoutSeconds = lockTimeoutSeconds;
		this.processId = processId;
	}

	@Override
	public AggregationMetadataStorage aggregationMetadataStorage(final String aggregationId,
	                                                             final AggregationMetadata aggregationMetadata,
	                                                             final AggregationStructure aggregationStructure) {
		return new AggregationMetadataStorage() {
			@Override
			public void createChunkId(ResultCallback<Long> callback) {
				callConcurrently(eventloop, executor, false, new Callable<Long>() {
					@Override
					public Long call() throws Exception {
						return doCreateChunkId();
					}
				}, callback);
			}

			@Override
			public void saveChunks(final List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
				runConcurrently(eventloop, executor, false, new Runnable() {
					@Override
					public void run() {
						Map<AggregationMetadata, String> idMap = new HashMap<>();
						idMap.put(aggregationMetadata, aggregationId);
						Multimap<AggregationMetadata, AggregationChunk.NewChunk> chunks = HashMultimap.create();
						chunks.putAll(aggregationMetadata, newChunks);
						doSaveNewChunks(DSL.using(jooqConfiguration), idMap, chunks);
					}
				}, callback);
			}

			@Override
			public void startConsolidation(final List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {
				runConcurrently(eventloop, executor, false, new Runnable() {
					@Override
					public void run() {
						doStartConsolidation(DSL.using(jooqConfiguration), chunksToConsolidate);
					}
				}, callback);
			}

			@Override
			public void loadChunks(final int lastRevisionId, final ResultCallback<LoadedChunks> callback) {
				callConcurrently(eventloop, executor, false, new Callable<LoadedChunks>() {
					@Override
					public LoadedChunks call() {
						return doLoadChunks(DSL.using(jooqConfiguration), aggregationId, aggregationMetadata, aggregationStructure, lastRevisionId);
					}
				}, callback);
			}

			@Override
			public void saveConsolidatedChunks(final List<AggregationChunk> originalChunks, final List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
				runConcurrently(eventloop, executor, false, new Runnable() {
					@Override
					public void run() {
						doSaveConsolidatedChunks(aggregationId, aggregationMetadata, originalChunks, consolidatedChunks);
					}
				}, callback);
			}
		};
	}

	public void truncateTables(DSLContext jooq) {
		jooq.truncate(AGGREGATION_DB_CHUNK).execute();
		jooq.truncate(AGGREGATION_DB_REVISION).execute();
	}

	public void truncateTables() {
		truncateTables(DSL.using(jooqConfiguration));
	}

	public int nextRevisionId(DSLContext jooq) {
		return jooq
				.insertInto(AGGREGATION_DB_REVISION)
				.defaultValues()
				.returning(AGGREGATION_DB_REVISION.ID)
				.fetchOne()
				.getId();
	}

	public long doCreateChunkId() {
		DSLContext jooq = DSL.using(jooqConfiguration);
		return jooq
				.insertInto(AGGREGATION_DB_CHUNK, AGGREGATION_DB_CHUNK.REVISION_ID,
						AGGREGATION_DB_CHUNK.COUNT)
				.values(0, 0)
				.returning(AGGREGATION_DB_CHUNK.ID)
				.fetchOne()
				.getId();
	}

	public void doSaveNewChunks(DSLContext jooq,
	                            Map<AggregationMetadata, String> idMap,
	                            Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunksWithMetadata) {
		int revisionId = nextRevisionId(jooq);
		doSaveNewChunks(jooq, revisionId, idMap, newChunksWithMetadata);
	}

	private void doSaveNewChunks(DSLContext jooq, final int revisionId,
	                             Map<AggregationMetadata, String> idMap,
	                             Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunksWithMetadata) {
		InsertQuery<AggregationDbChunkRecord> insertQuery = jooq.insertQuery(AGGREGATION_DB_CHUNK);

		for (AggregationMetadata aggregationMetadata : newChunksWithMetadata.keySet()) {
			String aggregationId = idMap.get(aggregationMetadata);
			for (AggregationChunk.NewChunk newChunk : newChunksWithMetadata.get(aggregationMetadata)) {
				insertQuery.newRecord();

				AggregationChunk chunk = createChunk(revisionId, newChunk);

				insertQuery.addValue(AGGREGATION_DB_CHUNK.ID, chunk.getChunkId());
				insertQuery.addValue(AGGREGATION_DB_CHUNK.AGGREGATION_ID, aggregationId);
				insertQuery.addValue(AGGREGATION_DB_CHUNK.REVISION_ID, chunk.getRevisionId());
				insertQuery.addValue(AGGREGATION_DB_CHUNK.KEYS, JOINER.join(aggregationMetadata.getKeys()));
				insertQuery.addValue(AGGREGATION_DB_CHUNK.FIELDS, JOINER.join(chunk.getFields()));
				insertQuery.addValue(AGGREGATION_DB_CHUNK.COUNT, chunk.getCount());
				insertQuery.addValue(AGGREGATION_DB_CHUNK.PROCESS_ID, processId);

				int keyLength = aggregationMetadata.getKeys().size();
				for (int d = 0; d < MAX_KEYS; d++) {
					Field<String> minField = AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_min").coerce(String.class);
					insertQuery.addValue(minField, d >= keyLength ? null : chunk.getMinPrimaryKey().values().get(d).toString());

					Field<String> maxField = AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_max").coerce(String.class);
					insertQuery.addValue(maxField, d >= keyLength ? null : chunk.getMaxPrimaryKey().values().get(d).toString());
				}
			}
		}

		ArrayList<Field<?>> updateFields = Lists.newArrayList(AGGREGATION_DB_CHUNK.fields());
		updateFields.remove(AGGREGATION_DB_CHUNK.ID);

		insertQuery.addValuesForUpdate(onDuplicateKeyUpdateValues(updateFields));
		insertQuery.onDuplicateKeyUpdate(true);
		insertQuery.execute();
	}

	@SuppressWarnings("unchecked")
	public LoadedChunks doLoadChunks(DSLContext jooq, String aggregationId, AggregationMetadata aggregationMetadata, AggregationStructure aggregationStructure, int lastRevisionId) {
		Record1<Integer> maxRevisionRecord = jooq
				.select(DSL.max(AGGREGATION_DB_CHUNK.REVISION_ID))
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregationId))
				.fetchOne();
		if (maxRevisionRecord.value1() == null) {
			return new LoadedChunks(lastRevisionId,
					Collections.<Long>emptyList(), Collections.<AggregationChunk>emptyList());
		}
		int newRevisionId = maxRevisionRecord.value1();

		List<Long> consolidatedChunkIds = new ArrayList<>();

		if (lastRevisionId != 0) {
			Result<Record1<Long>> consolidatedChunksRecords = jooq
					.select(AGGREGATION_DB_CHUNK.ID)
					.from(AGGREGATION_DB_CHUNK)
					.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregationId))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(lastRevisionId))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.le(newRevisionId))
					.fetch();

			for (Record1<Long> record : consolidatedChunksRecords) {
				Long sourceChunkId = record.getValue(AGGREGATION_DB_CHUNK.ID);
				consolidatedChunkIds.add(sourceChunkId);
			}
		}

		List<Field<?>> fieldsToFetch = new ArrayList<>(aggregationMetadata.getKeys().size() * 2 + 4);
		Collections.addAll(fieldsToFetch, AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.REVISION_ID,
				AGGREGATION_DB_CHUNK.FIELDS, AGGREGATION_DB_CHUNK.COUNT);
		for (int d = 0; d < aggregationMetadata.getKeys().size(); d++) {
			fieldsToFetch.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_min"));
			fieldsToFetch.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_max"));
		}

		List<Record> newChunkRecords = jooq
				.select(fieldsToFetch)
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregationId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
				.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.isNull())
				.unionAll(jooq.select(fieldsToFetch)
						.from(AGGREGATION_DB_CHUNK)
						.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregationId))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
						.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(newRevisionId)))
				.fetch();

		ArrayList<AggregationChunk> newChunks = new ArrayList<>();
		for (Record record : newChunkRecords) {
			Object[] minKeyArray = new Object[aggregationMetadata.getKeys().size()];
			Object[] maxKeyArray = new Object[aggregationMetadata.getKeys().size()];
			for (int d = 0; d < aggregationMetadata.getKeys().size(); d++) {
				String key = aggregationMetadata.getKeys().get(d);
				Class<?> type = aggregationStructure.getKeys().get(key).getDataType();
				minKeyArray[d] = record.getValue("d" + (d + 1) + "_min", type);
				maxKeyArray[d] = record.getValue("d" + (d + 1) + "_max", type);
			}

			AggregationChunk chunk = new AggregationChunk(record.getValue(AGGREGATION_DB_CHUNK.REVISION_ID),
					record.getValue(AGGREGATION_DB_CHUNK.ID).intValue(),
					newArrayList(SPLITTER.split(record.getValue(AGGREGATION_DB_CHUNK.FIELDS))),
					PrimaryKey.ofArray(minKeyArray),
					PrimaryKey.ofArray(maxKeyArray),
					record.getValue(AGGREGATION_DB_CHUNK.COUNT));
			newChunks.add(chunk);
		}
		return new LoadedChunks(newRevisionId, consolidatedChunkIds, newChunks);
	}

	private void doSaveConsolidatedChunks(final String aggregationId,
	                                      final AggregationMetadata aggregationMetadata,
	                                      final List<AggregationChunk> originalChunks,
	                                      final List<AggregationChunk.NewChunk> consolidatedChunks) {
		final Connection connection = jooqConfiguration.connectionProvider().acquire();
		final Configuration configurationWithConnection = jooqConfiguration.derive(connection);
		DSLContext jooq = DSL.using(configurationWithConnection);
		try {
			getCubeLock(jooq);
			jooq.transaction(new TransactionalRunnable() {
				@Override
				public void run(Configuration jooqConfiguration) throws Exception {
					DSLContext jooq = DSL.using(configurationWithConnection);

					final int revisionId = nextRevisionId(jooq);

					jooq.update(AGGREGATION_DB_CHUNK)
							.set(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID, revisionId)
							.set(AGGREGATION_DB_CHUNK.CONSOLIDATION_COMPLETED, currentTimestamp())
							.where(AGGREGATION_DB_CHUNK.ID.in(newArrayList(Iterables.transform(originalChunks,
									new Function<AggregationChunk, Long>() {
										@Override
										public Long apply(AggregationChunk chunk) {
											return chunk.getChunkId();
										}
									}))))
							.execute();

					Map<AggregationMetadata, String> idMap = new HashMap<>();
					idMap.put(aggregationMetadata, aggregationId);
					Multimap<AggregationMetadata, AggregationChunk.NewChunk> chunks = HashMultimap.create();
					chunks.putAll(aggregationMetadata, consolidatedChunks);
					doSaveNewChunks(jooq, revisionId, idMap, chunks);
				}
			});
		} finally {
			releaseCubeLock(jooq);
			jooqConfiguration.connectionProvider().release(connection);
		}
	}

	public void doStartConsolidation(final DSLContext jooq,
	                                 List<AggregationChunk> chunksToConsolidate) {
		jooq.update(AGGREGATION_DB_CHUNK)
				.set(AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED, currentTimestamp())
				.where(AGGREGATION_DB_CHUNK.ID.in(newArrayList(Iterables.transform(chunksToConsolidate, new Function<AggregationChunk, Long>() {
					@Override
					public Long apply(AggregationChunk chunk) {
						return chunk.getChunkId();
					}
				}))))
				.execute();
	}

	public void getCubeLock(DSLContext jooq) {
		Result<Record> result = jooq.fetch("SELECT GET_LOCK('" + LOCK_NAME + "', " + lockTimeoutSeconds + ")");
		if (!isValidLockingResult(result))
			throw new DataAccessException("Obtaining lock '" + LOCK_NAME + "' failed");
	}

	public void releaseCubeLock(DSLContext jooq) {
		try {
			Result<Record> result = jooq.fetch("SELECT RELEASE_LOCK('" + LOCK_NAME + "')");
			if (!isValidLockingResult(result))
				logger.error("Releasing lock '" + LOCK_NAME + "' did not complete correctly");
		} catch (Exception ignored) {
			// reported by jOOQ
		}
	}

	private static boolean isValidLockingResult(Result<Record> result) {
		if (result.get(0) == null || result.get(0).getValue(0) == null)
			return false;

		Object value = result.get(0).getValue(0);

		return (value instanceof Long && ((Long) value) == 1) || (value instanceof Integer && ((Integer) value) == 1);
	}

}
