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

package io.datakernel.aggregation_db;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.aggregation_db.sql.tables.records.AggregationDbChunkRecord;
import io.datakernel.aggregation_db.sql.tables.records.AggregationDbStructureRecord;
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
import static io.datakernel.aggregation_db.sql.tables.AggregationDbStructure.AGGREGATION_DB_STRUCTURE;
import static io.datakernel.aggregation_db.util.JooqUtils.onDuplicateKeyUpdateValues;
import static io.datakernel.async.AsyncCallbacks.callConcurrently;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static org.jooq.impl.DSL.currentTimestamp;

public class AggregationMetadataStorageSql implements AggregationMetadataStorage {
	private static final Logger logger = LoggerFactory.getLogger(AggregationMetadataStorageSql.class);

	private static final String LOCK_NAME = "cube_lock";
	private static final int DEFAULT_LOCK_TIMEOUT_SECONDS = 180;
	private static final int MAX_KEYS = 40;
	private static final Joiner JOINER = Joiner.on(' ');
	private static final Splitter SPLITTER = Splitter.on(' ').omitEmptyStrings();

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;
	private final int lockTimeoutSeconds;

	public AggregationMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration) {
		this(eventloop, executor, jooqConfiguration, DEFAULT_LOCK_TIMEOUT_SECONDS);
	}

	public AggregationMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration,
	                                     int lockTimeoutSeconds) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
		this.lockTimeoutSeconds = lockTimeoutSeconds;
	}

	public void truncateTables(DSLContext jooq) {
		jooq.truncate(AGGREGATION_DB_CHUNK).execute();
		jooq.truncate(AGGREGATION_DB_STRUCTURE).execute();
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

	@Override
	public void newChunkId(ResultCallback<Long> callback) {
		callConcurrently(eventloop, executor, false, new Callable<Long>() {
			@Override
			public Long call() throws Exception {
				return newChunkId();
			}
		}, callback);
	}

	@Override
	public long newChunkId() {
		DSLContext jooq = DSL.using(jooqConfiguration);
		return jooq
				.insertInto(AGGREGATION_DB_CHUNK, AGGREGATION_DB_CHUNK.REVISION_ID,
						AGGREGATION_DB_CHUNK.COUNT)
				.values(0, 0)
				.returning(AGGREGATION_DB_CHUNK.ID)
				.fetchOne()
				.getId();
	}

	public void saveAggregationMetadata(DSLContext jooq, Aggregation aggregation, AggregationStructure structure) {
		Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(structure))
				.create();

		jooq.insertInto(AGGREGATION_DB_STRUCTURE)
				.set(new AggregationDbStructureRecord(
						aggregation.getId(),
						JOINER.join(aggregation.getKeys()),
						JOINER.join(aggregation.getInputFields()),
						JOINER.join(aggregation.getOutputFields()),
						gson.toJson(aggregation.getAggregationPredicates())))
				.onDuplicateKeyIgnore()
				.execute();
	}

	@Override
	public void saveAggregationMetadata(final Aggregation aggregation, final AggregationStructure structure, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				saveAggregationMetadata(DSL.using(jooqConfiguration), aggregation, structure);
			}
		}, callback);
	}

	@Override
	public void saveChunks(final AggregationMetadata aggregationMetadata, final List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				DSLContext jooq = DSL.using(jooqConfiguration);
				saveNewChunks(jooq,
						ImmutableMultimap.<AggregationMetadata, AggregationChunk.NewChunk>builder()
								.putAll(aggregationMetadata, newChunks)
								.build());
			}
		}, callback);
	}

	public void saveNewChunks(DSLContext jooq,
	                          Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunksWithMetadata) {
		int revisionId = nextRevisionId(jooq);
		saveNewChunks(jooq, revisionId, newChunksWithMetadata);
	}

	public void saveNewChunks(DSLContext jooq, final int revisionId,
	                          Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunksWithMetadata) {
		InsertQuery<AggregationDbChunkRecord> insertQuery = jooq.insertQuery(AGGREGATION_DB_CHUNK);

		for (AggregationMetadata aggregationMetadata : newChunksWithMetadata.keySet()) {
			for (AggregationChunk.NewChunk newChunk : newChunksWithMetadata.get(aggregationMetadata)) {
				insertQuery.newRecord();

				AggregationChunk chunk = createChunk(revisionId, newChunk);

				insertQuery.addValue(AGGREGATION_DB_CHUNK.ID, chunk.getChunkId());
				insertQuery.addValue(AGGREGATION_DB_CHUNK.AGGREGATION_ID, aggregationMetadata.getId());
				insertQuery.addValue(AGGREGATION_DB_CHUNK.REVISION_ID, chunk.getRevisionId());
				insertQuery.addValue(AGGREGATION_DB_CHUNK.KEYS, JOINER.join(aggregationMetadata.getKeys()));
				insertQuery.addValue(AGGREGATION_DB_CHUNK.FIELDS, JOINER.join(chunk.getFields()));
				insertQuery.addValue(AGGREGATION_DB_CHUNK.COUNT, chunk.getCount());

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

	@Override
	public void loadChunks(final Aggregation aggregation, final int lastRevisionId, final ResultCallback<LoadedChunks> callback) {
		callConcurrently(eventloop, executor, false, new Callable<LoadedChunks>() {
			@Override
			public LoadedChunks call() {
				return loadChunks(DSL.using(jooqConfiguration), aggregation, lastRevisionId);
			}
		}, callback);
	}

	@SuppressWarnings("unchecked")
	public LoadedChunks loadChunks(DSLContext jooq, Aggregation aggregation, int lastRevisionId) {
		String indexId = aggregation.getId();
		Record1<Integer> maxRevisionRecord = jooq
				.select(DSL.max(AGGREGATION_DB_CHUNK.REVISION_ID))
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(indexId))
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
					.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(indexId))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(lastRevisionId))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.le(newRevisionId))
					.fetch();

			for (Record1<Long> record : consolidatedChunksRecords) {
				Long sourceChunkId = record.getValue(AGGREGATION_DB_CHUNK.ID);
				consolidatedChunkIds.add(sourceChunkId);
			}
		}

		List<Field<?>> fieldsToFetch = new ArrayList<>(aggregation.getKeys().size() * 2 + 4);
		Collections.addAll(fieldsToFetch, AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.REVISION_ID,
				AGGREGATION_DB_CHUNK.FIELDS, AGGREGATION_DB_CHUNK.COUNT);
		for (int d = 0; d < aggregation.getKeys().size(); d++) {
			fieldsToFetch.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_min"));
			fieldsToFetch.add(AGGREGATION_DB_CHUNK.field("d" + (d + 1) + "_max"));
		}

		List<Record> newChunkRecords = jooq
				.select(fieldsToFetch)
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(indexId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
				.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.isNull())
				.unionAll(jooq.select(fieldsToFetch)
						.from(AGGREGATION_DB_CHUNK)
						.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(indexId))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
						.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
						.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(newRevisionId)))
				.fetch();

		ArrayList<AggregationChunk> newChunks = new ArrayList<>();
		for (Record record : newChunkRecords) {
			Object[] minKeyArray = new Object[aggregation.getKeys().size()];
			Object[] maxKeyArray = new Object[aggregation.getKeys().size()];
			for (int d = 0; d < aggregation.getKeys().size(); d++) {
				String key = aggregation.getKeys().get(d);
				Class<?> type = aggregation.getStructure().getKeys().get(key).getDataType();
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

	@Override
	public void saveConsolidatedChunks(final AggregationMetadata aggregationMetadata, final List<AggregationChunk> originalChunks,
	                                   final List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				saveConsolidatedChunks(aggregationMetadata, originalChunks, consolidatedChunks);
			}
		}, callback);
	}

	public void saveConsolidatedChunks(final AggregationMetadata aggregationMetadata,
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
							.where(AGGREGATION_DB_CHUNK.ID.in(newArrayList(Iterables.transform(originalChunks, new Function<AggregationChunk, Long>() {
								@Override
								public Long apply(AggregationChunk chunk) {
									return chunk.getChunkId();
								}
							}))))
							.execute();

					saveNewChunks(jooq, revisionId, ImmutableMultimap.<AggregationMetadata, AggregationChunk.NewChunk>builder()
							.putAll(aggregationMetadata, consolidatedChunks)
							.build());
				}
			});
		} finally {
			releaseCubeLock(jooq);
			jooqConfiguration.connectionProvider().release(connection);
		}
	}

	@Override
	public void startConsolidation(final List<AggregationChunk> chunksToConsolidate,
	                               CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				startConsolidation(DSL.using(jooqConfiguration), chunksToConsolidate);
			}
		}, callback);
	}

	public void startConsolidation(final DSLContext jooq,
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
