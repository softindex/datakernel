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
import static org.jooq.impl.DSL.currentTimestamp;

public class CubeMetadataStorageSql implements CubeMetadataStorage {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String LOCK_NAME = "cube_lock";
	private static final int DEFAULT_LOCK_TIMEOUT_SECONDS = 180;
	private static final Joiner JOINER = Joiner.on(' ');
	private static final Splitter SPLITTER = Splitter.on(' ').omitEmptyStrings();

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;
	private final int lockTimeoutSeconds;
	private final String processId;

	private CubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration, String processId) {
		this(eventloop, executor, jooqConfiguration, DEFAULT_LOCK_TIMEOUT_SECONDS, processId);
	}

	private CubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration,
	                               int lockTimeoutSeconds, String processId) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
		this.lockTimeoutSeconds = lockTimeoutSeconds;
		this.processId = processId;
	}

	public static CubeMetadataStorageSql create(Eventloop eventloop, ExecutorService executor,
	                                            Configuration jooqConfiguration, String processId) {
		return new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration, processId);
	}

	public static CubeMetadataStorageSql create(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration,
	                                            int lockTimeoutSeconds, String processId) {
		return new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration, lockTimeoutSeconds, processId);
	}

	@Override
	public AggregationMetadataStorage aggregationMetadataStorage(final String aggregationId,
	                                                             final AggregationMetadata aggregationMetadata,
	                                                             final AggregationStructure aggregationStructure) {
		return new AggregationMetadataStorage() {
			@Override
			public void createChunkId(ResultCallback<Long> callback) {
				eventloop.callConcurrently(executor, new Callable<Long>() {
					@Override
					public Long call() throws Exception {
						return doCreateChunkId();
					}
				}, callback);
			}

			@Override
			public void saveChunks(final List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
				eventloop.runConcurrently(executor, new Runnable() {
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
				eventloop.runConcurrently(executor, new Runnable() {
					@Override
					public void run() {
						doStartConsolidation(DSL.using(jooqConfiguration), chunksToConsolidate);
					}
				}, callback);
			}

			@Override
			public void loadChunks(final int lastRevisionId, final ResultCallback<LoadedChunks> callback) {
				eventloop.callConcurrently(executor, new Callable<LoadedChunks>() {
					@Override
					public LoadedChunks call() {
						return doLoadChunks(DSL.using(jooqConfiguration), aggregationId, aggregationMetadata,
								aggregationStructure, lastRevisionId);
					}
				}, callback);
			}

			@Override
			public void saveConsolidatedChunks(final List<AggregationChunk> originalChunks,
			                                   final List<AggregationChunk.NewChunk> consolidatedChunks,
			                                   CompletionCallback callback) {
				eventloop.runConcurrently(executor, new Runnable() {
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

	private static int getMaxKeyLength(Iterable<AggregationMetadata> aggregations) {
		int maxKeyLength = 0;
		for (AggregationMetadata aggregation : aggregations) {
			maxKeyLength = Math.max(maxKeyLength, aggregation.getKeys().size());
		}
		return maxKeyLength;
	}

	private static Collection<Field<?>> getChunkUpdateFields(int maxKeyLength) {
		List<Field<?>> updateFields = new ArrayList<>(maxKeyLength * 2 + 6);

		updateFields.add(AGGREGATION_DB_CHUNK.AGGREGATION_ID);
		updateFields.add(AGGREGATION_DB_CHUNK.REVISION_ID);
		updateFields.add(AGGREGATION_DB_CHUNK.KEYS);
		updateFields.add(AGGREGATION_DB_CHUNK.FIELDS);
		updateFields.add(AGGREGATION_DB_CHUNK.COUNT);
		updateFields.add(AGGREGATION_DB_CHUNK.PROCESS_ID);

		for (int d = 0; d < maxKeyLength; d++) {
			updateFields.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_min"));
			updateFields.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_max"));
		}

		return updateFields;
	}

	private void doSaveNewChunks(DSLContext jooq, final int revisionId,
	                             Map<AggregationMetadata, String> idMap,
	                             Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunksWithMetadata) {
		InsertQuery<AggregationDbChunkRecord> insertQuery = jooq.insertQuery(AGGREGATION_DB_CHUNK);

		Set<AggregationMetadata> aggregationMetadatas = newChunksWithMetadata.keySet();

		int maxKeyLength = getMaxKeyLength(aggregationMetadatas);

		for (AggregationMetadata aggregationMetadata : aggregationMetadatas) {
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
				for (int d = 0; d < maxKeyLength; d++) {
					Field<String> minField = AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_min").coerce(String.class);
					insertQuery.addValue(minField, d >= keyLength ? null : chunk.getMinPrimaryKey().values().get(d).toString());

					Field<String> maxField = AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_max").coerce(String.class);
					insertQuery.addValue(maxField, d >= keyLength ? null : chunk.getMaxPrimaryKey().values().get(d).toString());
				}
			}
		}

		Collection<Field<?>> updateFields = getChunkUpdateFields(maxKeyLength);
		insertQuery.addValuesForUpdate(onDuplicateKeyUpdateValues(updateFields));
		insertQuery.onDuplicateKeyUpdate(true);
		insertQuery.execute();
	}

	@Override
	public void loadChunks(final int lastRevisionId, final Map<String, AggregationMetadata> aggregations,
	                       final AggregationStructure aggregationStructure, ResultCallback<CubeLoadedChunks> callback) {
		eventloop.callConcurrently(executor, new Callable<CubeLoadedChunks>() {
			@Override
			public CubeLoadedChunks call() {
				return doLoadChunks(DSL.using(jooqConfiguration), aggregations, aggregationStructure, lastRevisionId);
			}
		}, callback);
	}

	private static class ChunkKey {
		private final Object[] minKeyArray;
		private final Object[] maxKeyArray;

		public ChunkKey(Object[] minKeyArray, Object[] maxKeyArray) {
			this.minKeyArray = minKeyArray;
			this.maxKeyArray = maxKeyArray;
		}
	}

	private static ChunkKey getChunkKey(Record record, AggregationMetadata aggregationMetadata,
	                                    AggregationStructure aggregationStructure) {
		Object[] minKeyArray = new Object[aggregationMetadata.getKeys().size()];
		Object[] maxKeyArray = new Object[aggregationMetadata.getKeys().size()];

		for (int d = 0; d < aggregationMetadata.getKeys().size(); d++) {
			String key = aggregationMetadata.getKeys().get(d);
			Class<?> type = aggregationStructure.getKeys().get(key).getDataType();
			minKeyArray[d] = record.getValue("d" + (d + 1) + "_min", type);
			maxKeyArray[d] = record.getValue("d" + (d + 1) + "_max", type);
		}

		return new ChunkKey(minKeyArray, maxKeyArray);
	}

	private static List<AggregationChunk> transformToChunks(List<Record> chunkRecords,
	                                                        AggregationMetadata aggregationMetadata,
	                                                        AggregationStructure aggregationStructure) {
		List<AggregationChunk> chunks = new ArrayList<>();

		for (Record record : chunkRecords) {
			ChunkKey chunkKey = getChunkKey(record, aggregationMetadata, aggregationStructure);
			AggregationChunk chunk = AggregationChunk.create(record.getValue(AGGREGATION_DB_CHUNK.REVISION_ID),
					record.getValue(AGGREGATION_DB_CHUNK.ID).intValue(),
					newArrayList(SPLITTER.split(record.getValue(AGGREGATION_DB_CHUNK.FIELDS))),
					PrimaryKey.ofArray(chunkKey.minKeyArray),
					PrimaryKey.ofArray(chunkKey.maxKeyArray),
					record.getValue(AGGREGATION_DB_CHUNK.COUNT));
			chunks.add(chunk);
		}

		return chunks;
	}

	private static Map<String, List<AggregationChunk>> transformToAggregationChunks(List<Record> chunkRecords,
	                                                                                Map<String, AggregationMetadata> aggregations,
	                                                                                AggregationStructure aggregationStructure) {
		Map<String, List<AggregationChunk>> aggregationChunks = new HashMap<>();

		for (Record record : chunkRecords) {
			String aggregationId = record.getValue(AGGREGATION_DB_CHUNK.AGGREGATION_ID);
			AggregationMetadata aggregationMetadata = aggregations.get(aggregationId);

			if (aggregationMetadata == null) // aggregation was removed
				continue;

			ChunkKey chunkKey = getChunkKey(record, aggregationMetadata, aggregationStructure);
			AggregationChunk chunk = AggregationChunk.create(record.getValue(AGGREGATION_DB_CHUNK.REVISION_ID),
					record.getValue(AGGREGATION_DB_CHUNK.ID).intValue(),
					newArrayList(SPLITTER.split(record.getValue(AGGREGATION_DB_CHUNK.FIELDS))),
					PrimaryKey.ofArray(chunkKey.minKeyArray),
					PrimaryKey.ofArray(chunkKey.maxKeyArray),
					record.getValue(AGGREGATION_DB_CHUNK.COUNT));

			if (aggregationChunks.containsKey(aggregationId))
				aggregationChunks.get(aggregationId).add(chunk);
			else
				aggregationChunks.put(aggregationId, newArrayList(chunk));
		}

		return aggregationChunks;
	}

	private static List<Field<?>> getChunkSelectFields(int maxKeyLength) {
		List<Field<?>> fieldsToFetch = new ArrayList<>(maxKeyLength * 2 + 4);
		Collections.addAll(fieldsToFetch, AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.REVISION_ID,
				AGGREGATION_DB_CHUNK.FIELDS, AGGREGATION_DB_CHUNK.COUNT);
		addKeyColumns(fieldsToFetch, maxKeyLength);
		return fieldsToFetch;
	}

	private static List<Field<?>> getChunkSelectFieldsWithAggregationId(int maxKeyLength) {
		List<Field<?>> fieldsToSelect = new ArrayList<>(maxKeyLength * 2 + 5);
		Collections.addAll(fieldsToSelect, AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.AGGREGATION_ID,
				AGGREGATION_DB_CHUNK.REVISION_ID, AGGREGATION_DB_CHUNK.FIELDS, AGGREGATION_DB_CHUNK.COUNT);
		addKeyColumns(fieldsToSelect, maxKeyLength);
		return fieldsToSelect;
	}

	private static void addKeyColumns(List<Field<?>> fields, int maxKeyLength) {
		for (int d = 0; d < maxKeyLength; d++) {
			fields.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_min"));
			fields.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_max"));
		}
	}

	public CubeLoadedChunks doLoadChunks(DSLContext jooq, Map<String, AggregationMetadata> aggregations,
	                                     AggregationStructure aggregationStructure, int lastRevisionId) {
		Record1<Integer> maxRevisionRecord = jooq
				.select(DSL.max(AGGREGATION_DB_CHUNK.REVISION_ID))
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.notEqual(""))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.fetchOne();
		if (maxRevisionRecord.value1() == null) {
			return new CubeLoadedChunks(lastRevisionId, Collections.<String, List<Long>>emptyMap(),
					Collections.<String, List<AggregationChunk>>emptyMap());
		}
		int newRevisionId = maxRevisionRecord.value1();

		Map<String, List<Long>> consolidatedChunkIds;
		if (lastRevisionId != 0) {
			consolidatedChunkIds = jooq
					.select(AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.AGGREGATION_ID)
					.from(AGGREGATION_DB_CHUNK)
					.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.notEqual(""))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(lastRevisionId))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.le(newRevisionId))
					.fetchGroups(AGGREGATION_DB_CHUNK.AGGREGATION_ID, AGGREGATION_DB_CHUNK.ID);
		} else {
			consolidatedChunkIds = Collections.emptyMap();
		}

		int maxKeyLength = getMaxKeyLength(aggregations.values());
		List<Field<?>> fieldsToSelect = getChunkSelectFieldsWithAggregationId(maxKeyLength);

		List<Record> newChunkRecords = jooq
				.select(fieldsToSelect)
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.notEqual(""))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
				.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.isNull())
				.unionAll(jooq.select(fieldsToSelect)
						.from(AGGREGATION_DB_CHUNK)
						.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.notEqual(""))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
						.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(newRevisionId)))
				.fetch();

		Map<String, List<AggregationChunk>> newChunks = transformToAggregationChunks(newChunkRecords, aggregations, aggregationStructure);

		return new CubeLoadedChunks(newRevisionId, consolidatedChunkIds, newChunks);
	}

	public LoadedChunks doLoadChunks(DSLContext jooq, String aggregationId,
	                                 AggregationMetadata aggregationMetadata,
	                                 AggregationStructure aggregationStructure, int lastRevisionId) {
		Record1<Integer> maxRevisionRecord = jooq
				.select(DSL.max(AGGREGATION_DB_CHUNK.REVISION_ID))
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregationId))
				.fetchOne();
		if (maxRevisionRecord.value1() == null) {
			return new LoadedChunks(lastRevisionId, Collections.<Long>emptyList(), Collections.<AggregationChunk>emptyList());
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

		List<Field<?>> fieldsToSelect = getChunkSelectFields(aggregationMetadata.getKeys().size());

		List<Record> newChunkRecords = jooq
				.select(fieldsToSelect)
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregationId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
				.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.isNull())
				.unionAll(jooq.select(fieldsToSelect)
						.from(AGGREGATION_DB_CHUNK)
						.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregationId))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
						.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(newRevisionId)))
				.fetch();

		List<AggregationChunk> newChunks = transformToChunks(newChunkRecords, aggregationMetadata, aggregationStructure);

		return new LoadedChunks(newRevisionId, consolidatedChunkIds, newChunks);
	}

	private void doSaveConsolidatedChunks(final String aggregationId,
	                                      final AggregationMetadata aggregationMetadata,
	                                      final List<AggregationChunk> originalChunks,
	                                      final List<AggregationChunk.NewChunk> consolidatedChunks) {
		executeExclusiveTransaction(new TransactionalRunnable() {
			@Override
			public void run(Configuration configuration) throws Exception {
				DSLContext jooq = DSL.using(configuration);

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

	public void executeExclusiveTransaction(TransactionalRunnable transactionalRunnable) {
		Connection connection = jooqConfiguration.connectionProvider().acquire();
		Configuration configurationWithConnection = jooqConfiguration.derive(connection);
		DSLContext jooq = DSL.using(configurationWithConnection);
		boolean gotLock = false;
		try {
			gotLock = getCubeLock(jooq);
			jooq.transaction(transactionalRunnable);
		} finally {
			if (gotLock)
				releaseCubeLock(jooq);

			jooqConfiguration.connectionProvider().release(connection);
		}
	}

	private boolean getCubeLock(DSLContext jooq) {
		Result<Record> result = jooq.fetch("SELECT GET_LOCK('" + LOCK_NAME + "', " + lockTimeoutSeconds + ")");

		if (!isValidLockingResult(result))
			throw new DataAccessException("Obtaining lock '" + LOCK_NAME + "' failed");

		return true;
	}

	private void releaseCubeLock(DSLContext jooq) {
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
