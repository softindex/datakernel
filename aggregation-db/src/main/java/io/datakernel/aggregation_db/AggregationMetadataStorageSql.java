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
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.aggregation_db.sql.tables.AggregationDbRevision;
import io.datakernel.aggregation_db.sql.tables.records.AggregationDbChunkRecord;
import io.datakernel.aggregation_db.sql.tables.records.AggregationDbStructureRecord;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static com.google.common.collect.Multimaps.transformValues;
import static io.datakernel.aggregation_db.AggregationChunk.createCommitChunk;
import static io.datakernel.aggregation_db.AggregationChunk.createConsolidateChunk;
import static io.datakernel.aggregation_db.sql.tables.AggregationDbChunk.AGGREGATION_DB_CHUNK;
import static io.datakernel.aggregation_db.sql.tables.AggregationDbStructure.AGGREGATION_DB_STRUCTURE;
import static io.datakernel.async.AsyncCallbacks.callConcurrently;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static org.jooq.impl.DSL.*;

public class AggregationMetadataStorageSql implements AggregationMetadataStorage {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;

	public AggregationMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
	}

	public void truncateTables(DSLContext jooq) {
		jooq.truncate(AGGREGATION_DB_CHUNK).execute();
		jooq.truncate(AGGREGATION_DB_STRUCTURE).execute();
		jooq.truncate(AggregationDbRevision.AGGREGATION_DB_REVISION).execute();
	}

	public void truncateTables() {
		truncateTables(DSL.using(jooqConfiguration));
	}

	public int nextRevisionId(DSLContext jooq) {
		Integer id = jooq
				.insertInto(AggregationDbRevision.AGGREGATION_DB_REVISION)
				.defaultValues()
				.returning(AggregationDbRevision.AGGREGATION_DB_REVISION.ID)
				.fetchOne()
				.getId();
		return id;
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
		Long id = jooq
				.insertInto(AGGREGATION_DB_CHUNK, AGGREGATION_DB_CHUNK.REVISION_ID, AGGREGATION_DB_CHUNK.MIN_REVISION_ID,
						AGGREGATION_DB_CHUNK.MAX_REVISION_ID, AGGREGATION_DB_CHUNK.COUNT)
				.values(0, 0, 0, 0)
				.returning(AGGREGATION_DB_CHUNK.ID)
				.fetchOne()
				.getId();
		return id;
	}

	private void saveAggregationMetadata(DSLContext jooq, Aggregation aggregation, AggregationStructure structure) {
		Joiner joiner = Joiner.on(' ');

		Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(structure))
				.create();

		jooq.insertInto(AGGREGATION_DB_STRUCTURE)
				.set(new AggregationDbStructureRecord(
						aggregation.getId(),
						joiner.join(aggregation.getKeys()),
						joiner.join(aggregation.getInputFields()),
						joiner.join(aggregation.getOutputFields()),
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

				final int revisionId = nextRevisionId(jooq);

				saveChunks(jooq, aggregationMetadata, transform(newChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
					@Override
					public AggregationChunk apply(AggregationChunk.NewChunk newChunk) {
						return createCommitChunk(revisionId, newChunk);
					}
				}));
			}
		}, callback);
	}

	@Override
	public int loadChunks(Aggregation aggregation, int lastRevisionId, int maxRevisionId) {
		return loadChunks(DSL.using(jooqConfiguration), aggregation, lastRevisionId, maxRevisionId);
	}

	private void saveChunks(DSLContext jooq, AggregationMetadata aggregationMetadata, Collection<AggregationChunk> chunks) {
		for (AggregationChunk chunk : chunks) {
			AggregationDbChunkRecord record = new AggregationDbChunkRecord();
			record.setId(chunk.getChunkId());
			record.setAggregationId(aggregationMetadata.getId());
			record.setRevisionId(chunk.getRevisionId());
			record.setMinRevisionId(chunk.getMinRevisionId());
			record.setMaxRevisionId(chunk.getMaxRevisionId());
			record.setKeys(Joiner.on(' ').join(aggregationMetadata.getKeys()));
			record.setFields(Joiner.on(' ').join(chunk.getFields()));
			record.setCount(chunk.getCount());
			record.setCreated(new Timestamp(Calendar.getInstance().getTime().getTime()));

			Map<Field<?>, Object> map = new LinkedHashMap<>();
			int size = record.size();
			for (int i = 1; i < size; i++) {
				map.put(record.field(i), record.getValue(i));
			}

			for (int d = 0; d < aggregationMetadata.getKeys().size(); d++) {
				map.put(field("d" + (d + 1) + "_min"), chunk.getMinPrimaryKey().values().get(d).toString());
				map.put(field("d" + (d + 1) + "_max"), chunk.getMaxPrimaryKey().values().get(d).toString());
			}

			jooq.update(AGGREGATION_DB_CHUNK).set(map).where(AGGREGATION_DB_CHUNK.ID.equal(chunk.getChunkId())).execute();
		}
	}

	public void saveChunks(DSLContext jooq, Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks) {
		final int revisionId = nextRevisionId(jooq);

		doSaveChunks(jooq, transformValues(newChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
			@Override
			public AggregationChunk apply(AggregationChunk.NewChunk newChunk) {
				return createCommitChunk(revisionId, newChunk);
			}
		}));
	}

	public void doSaveChunks(DSLContext jooq, Multimap<AggregationMetadata, AggregationChunk> chunks) {
		for (AggregationMetadata aggregationMetadata : chunks.keySet()) {
			saveChunks(jooq, aggregationMetadata, chunks.get(aggregationMetadata));
		}
	}

	@Override
	public void loadChunks(final Aggregation aggregation, final int lastRevisionId, final int maxRevisionId, ResultCallback<Integer> callback) {
		callConcurrently(eventloop, executor, false, new Callable<Integer>() {
			@Override
			public Integer call() {
				return loadChunks(DSL.using(jooqConfiguration), aggregation, lastRevisionId, maxRevisionId);
			}
		}, callback);
	}

	private int loadChunks(DSLContext jooq, Aggregation aggregation, int lastRevisionId, int maxRevisionId) {
		Record1<Integer> maxRevisionRecord = jooq
				.select(DSL.max(AggregationDbRevision.AGGREGATION_DB_REVISION.ID))
				.from(AggregationDbRevision.AGGREGATION_DB_REVISION)
				.where(AggregationDbRevision.AGGREGATION_DB_REVISION.ID.gt(lastRevisionId)
						.and(AggregationDbRevision.AGGREGATION_DB_REVISION.ID.le(maxRevisionId)))
				.fetchOne();
		if (maxRevisionRecord.value1() == null)
			return lastRevisionId;
		int newRevisionId = maxRevisionRecord.value1();

		Multimap<Integer, Long> sourceChunkIdsMultimap = LinkedListMultimap.create();
		Map<Integer, long[]> sourceChunkIdsMap = new HashMap<>();
		String indexId = aggregation.getId();

		if (lastRevisionId != 0) {
			Result<Record4<Long, Integer, Timestamp, Timestamp>> consolidatedChunksRecords = jooq
					.select(AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID,
							AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED, AGGREGATION_DB_CHUNK.CONSOLIDATION_COMPLETED)
					.from(AGGREGATION_DB_CHUNK)
					.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(indexId))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(lastRevisionId))
					.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.le(newRevisionId))
					.fetch();

			for (Record4<Long, Integer, Timestamp, Timestamp> record : consolidatedChunksRecords) {
				Integer consolidatedRevisionId = record.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID);
				Long sourceChunkId = record.getValue(AGGREGATION_DB_CHUNK.ID);
				Timestamp consolidationStarted = record.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED);
				Timestamp consolidationCompleted = record.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATION_COMPLETED);
				sourceChunkIdsMultimap.put(consolidatedRevisionId, sourceChunkId);

				AggregationChunk chunk = aggregation.getChunks().get(sourceChunkId);

				if (chunk != null) {
					chunk.setConsolidatedRevisionId(consolidatedRevisionId);
					chunk.setConsolidationStarted(consolidationStarted);
					chunk.setConsolidationCompleted(consolidationCompleted);
				}
			}

			for (Integer revisionId : sourceChunkIdsMultimap.keySet()) {
				sourceChunkIdsMap.put(revisionId, Longs.toArray(sourceChunkIdsMultimap.get(revisionId)));
			}
		}

		List<Record> newChunkRecords = jooq
				.select()
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(indexId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
				.and(lastRevisionId == 0
						? AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.isNull().or(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(newRevisionId))
						: trueCondition())
				.fetch();

		Splitter splitter = Splitter.on(' ').omitEmptyStrings();
		for (Record record : newChunkRecords) {
			boolean isConsolidated = record.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID) != null &&
					record.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID) <= newRevisionId;

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
					record.getValue(AGGREGATION_DB_CHUNK.AGGREGATION_ID),
					record.getValue(AGGREGATION_DB_CHUNK.MIN_REVISION_ID),
					record.getValue(AGGREGATION_DB_CHUNK.MAX_REVISION_ID),
					sourceChunkIdsMap.get(record.getValue(AGGREGATION_DB_CHUNK.REVISION_ID)),
					newArrayList(splitter.split(record.getValue(AGGREGATION_DB_CHUNK.FIELDS))),
					PrimaryKey.ofArray(minKeyArray),
					PrimaryKey.ofArray(maxKeyArray),
					record.getValue(AGGREGATION_DB_CHUNK.COUNT));
			aggregation.addChunk(chunk);
			if (!isConsolidated) {
				aggregation.addToIndex(chunk);
			}
		}

		for (Long chunkId : sourceChunkIdsMultimap.values()) {
			AggregationChunk chunk = aggregation.getChunks().get(chunkId);
			aggregation.removeFromIndex(chunk);
		}

		return newRevisionId;
	}

	@Override
	public void reloadAllChunkConsolidations(final Aggregation aggregation, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				reloadAllChunkConsolidations(using(jooqConfiguration), aggregation);
			}
		}, callback);
	}

	private void reloadAllChunkConsolidations(DSLContext jooq, Aggregation aggregation) {
		Result<Record4<Long, Integer, Timestamp, Timestamp>> chunkRecords = jooq
				.select(AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID,
						AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED, AGGREGATION_DB_CHUNK.CONSOLIDATION_COMPLETED)
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(aggregation.getId()))
				.fetch();

		for (Record4<Long, Integer, Timestamp, Timestamp> chunkRecord : chunkRecords) {
			AggregationChunk chunk = aggregation.getChunks().get(chunkRecord.getValue(AGGREGATION_DB_CHUNK.ID));
			if (chunk != null) {
				chunk.setConsolidatedRevisionId(chunkRecord.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID));
				chunk.setConsolidationStarted(chunkRecord.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED));
				chunk.setConsolidationCompleted(chunkRecord.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATION_COMPLETED));
			}
		}
	}

	public void saveConsolidatedChunks(DSLContext jooq, final Aggregation aggregation,
	                                   final AggregationMetadata aggregationMetadata,
	                                   final List<AggregationChunk> originalChunks,
	                                   final List<AggregationChunk.NewChunk> consolidatedChunks) {
		jooq.transaction(new TransactionalRunnable() {
			@Override
			public void run(Configuration jooqConfiguration) throws Exception {
				DSLContext jooq = DSL.using(jooqConfiguration);

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

				saveChunks(jooq, aggregationMetadata, newArrayList(Iterables.transform(consolidatedChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
					@Override
					public AggregationChunk apply(AggregationChunk.NewChunk newChunk) {
						return createConsolidateChunk(revisionId, originalChunks, newChunk);
					}
				})));
			}
		});

		aggregation.setLastRevisionId(loadChunks(jooq, aggregation, aggregation.getLastRevisionId(), Integer.MAX_VALUE));
	}

	@Override
	public void saveConsolidatedChunks(final Aggregation aggregation, final AggregationMetadata aggregationMetadata, final List<AggregationChunk> originalChunks,
	                                   final List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				saveConsolidatedChunks(DSL.using(jooqConfiguration), aggregation, aggregationMetadata, originalChunks, consolidatedChunks);
			}
		}, callback);
	}

	@Override
	public void performConsolidation(final Aggregation aggregation, final Function<List<Long>, List<AggregationChunk>> chunkConsolidator,
	                                 CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				performConsolidation(DSL.using(jooqConfiguration), aggregation, chunkConsolidator);
			}
		}, callback);
	}

	private void performConsolidation(final DSLContext jooq, final Aggregation aggregation,
	                                  final Function<List<Long>, List<AggregationChunk>> chunkConsolidator) {
		lockTablesForConsolidation(jooq);

		refreshChunkConsolidations(jooq, aggregation);

		List<AggregationChunk> chunksToConsolidate = chunkConsolidator.apply(aggregation.getIdsOfChunksAvailableForConsolidation());

		if (chunksToConsolidate != null && chunksToConsolidate.size() > 0) {
			writeConsolidationStartTimestamp(jooq, chunksToConsolidate);
		}

		unlockTablesForConsolidation(jooq);
	}

	private void refreshChunkConsolidations(DSLContext jooq, Aggregation aggregation) {
		Result<Record3<Long, Integer, Timestamp>> chunkRecords = jooq
				.select(AGGREGATION_DB_CHUNK.ID, AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID, AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED)
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.ID.in(aggregation.getIdsOfNotConsolidatedChunks()))
				.fetch();

		for (Record3<Long, Integer, Timestamp> chunkRecord : chunkRecords) {
			AggregationChunk chunk = aggregation.getChunks().get(chunkRecord.getValue(AGGREGATION_DB_CHUNK.ID));
			if (chunk != null) {
				chunk.setConsolidatedRevisionId(chunkRecord.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID));
				chunk.setConsolidationStarted(chunkRecord.getValue(AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED));
			}
		}
	}

	private void writeConsolidationStartTimestamp(DSLContext jooq, List<AggregationChunk> chunks) {
		jooq.update(AGGREGATION_DB_CHUNK)
				.set(AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED, currentTimestamp())
				.where(AGGREGATION_DB_CHUNK.ID.in(newArrayList(Iterables.transform(chunks, new Function<AggregationChunk, Long>() {
					@Override
					public Long apply(AggregationChunk chunk) {
						return chunk.getChunkId();
					}
				}))))
				.execute();
	}

	private void lockTablesForConsolidation(DSLContext jooq) {
		jooq.execute("LOCK TABLES AGGREGATION_DB_CHUNK WRITE");
	}

	private void unlockTablesForConsolidation(DSLContext jooq) {
		jooq.execute("UNLOCK TABLES");
	}

	@Override
	public void refreshNotConsolidatedChunks(final Aggregation aggregation, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				refreshChunkConsolidations(DSL.using(jooqConfiguration), aggregation);
			}
		}, callback);
	}

	@Override
	public void removeChunk(final long chunkId, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				removeChunk(using(jooqConfiguration), chunkId);
			}
		}, callback);
	}

	private void removeChunk(DSLContext jooq, long chunkId) {
		jooq.delete(AGGREGATION_DB_CHUNK).where(AGGREGATION_DB_CHUNK.ID.eq(chunkId)).execute();
	}
}
