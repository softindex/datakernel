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
import com.google.common.collect.Multimap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static com.google.common.collect.Multimaps.transformValues;
import static io.datakernel.aggregation_db.AggregationChunk.createChunk;
import static io.datakernel.aggregation_db.sql.tables.AggregationDbChunk.AGGREGATION_DB_CHUNK;
import static io.datakernel.aggregation_db.sql.tables.AggregationDbStructure.AGGREGATION_DB_STRUCTURE;
import static io.datakernel.async.AsyncCallbacks.callConcurrently;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.field;

public class AggregationMetadataStorageSql implements AggregationMetadataStorage {
	private static final Logger logger = LoggerFactory.getLogger(AggregationMetadataStorageSql.class);

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
		return jooq
				.insertInto(AggregationDbRevision.AGGREGATION_DB_REVISION)
				.defaultValues()
				.returning(AggregationDbRevision.AGGREGATION_DB_REVISION.ID)
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
				.insertInto(AGGREGATION_DB_CHUNK, AGGREGATION_DB_CHUNK.REVISION_ID, AGGREGATION_DB_CHUNK.MIN_REVISION_ID,
						AGGREGATION_DB_CHUNK.MAX_REVISION_ID, AGGREGATION_DB_CHUNK.COUNT)
				.values(0, 0, 0, 0)
				.returning(AGGREGATION_DB_CHUNK.ID)
				.fetchOne()
				.getId();
	}

	public void saveAggregationMetadata(DSLContext jooq, Aggregation aggregation, AggregationStructure structure) {
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
						return createChunk(revisionId, newChunk);
					}
				}));
			}
		}, callback);
	}

	public void saveChunks(DSLContext jooq, AggregationMetadata aggregationMetadata, Collection<AggregationChunk> chunks) {
		for (AggregationChunk chunk : chunks) {
			AggregationDbChunkRecord record = new AggregationDbChunkRecord();
			record.setId(chunk.getChunkId());
			record.setAggregationId(aggregationMetadata.getId());
			record.setRevisionId(chunk.getRevisionId());
			record.setMinRevisionId(chunk.getRevisionId()); // TODO (dtkachenko): remove unused min-max fields
			record.setMaxRevisionId(chunk.getRevisionId());
			record.setKeys(Joiner.on(' ').join(aggregationMetadata.getKeys()));
			record.setFields(Joiner.on(' ').join(chunk.getFields()));
			record.setCount(chunk.getCount());
			record.setCreated(new Timestamp(Calendar.getInstance().getTime().getTime()));

			Map<Field<?>, Object> fields = new LinkedHashMap<>();
			int size = record.size();
			for (int i = 1; i < size; i++) {
				fields.put(record.field(i), record.getValue(i));
			}

			for (int d = 0; d < aggregationMetadata.getKeys().size(); d++) {
				fields.put(field("d" + (d + 1) + "_min"), chunk.getMinPrimaryKey().values().get(d).toString());
				fields.put(field("d" + (d + 1) + "_max"), chunk.getMaxPrimaryKey().values().get(d).toString());
			}

			jooq.update(AGGREGATION_DB_CHUNK).set(fields).where(AGGREGATION_DB_CHUNK.ID.equal(chunk.getChunkId())).execute();
		}
	}

	public void saveChunks(DSLContext jooq,
	                       Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunksWithMetadata) {
		final int revisionId = nextRevisionId(jooq);

		Multimap<AggregationMetadata, AggregationChunk> chunksWithMetadata = transformValues(newChunksWithMetadata,
				new Function<AggregationChunk.NewChunk, AggregationChunk>() {
					@Override
					public AggregationChunk apply(AggregationChunk.NewChunk newChunk) {
						return createChunk(revisionId, newChunk);
					}
				});
		for (AggregationMetadata chunkMetadata : chunksWithMetadata.keySet()) {
			saveChunks(jooq, chunkMetadata, chunksWithMetadata.get(chunkMetadata));
		}
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

		List<Record> newChunkRecords = jooq
				.select()
				.from(AGGREGATION_DB_CHUNK)
				.where(AGGREGATION_DB_CHUNK.AGGREGATION_ID.equal(indexId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_DB_CHUNK.REVISION_ID.le(newRevisionId))
				.and(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.isNull().or(AGGREGATION_DB_CHUNK.CONSOLIDATED_REVISION_ID.gt(newRevisionId)))
				.fetch();

		Splitter splitter = Splitter.on(' ').omitEmptyStrings();

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
					newArrayList(splitter.split(record.getValue(AGGREGATION_DB_CHUNK.FIELDS))),
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
				saveConsolidatedChunks(DSL.using(jooqConfiguration), aggregationMetadata, originalChunks, consolidatedChunks);
			}
		}, callback);
	}

	public void saveConsolidatedChunks(DSLContext jooq, final AggregationMetadata aggregationMetadata,
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
						return createChunk(revisionId, newChunk);
					}
				})));
			}
		});
	}

	@Override
	public void startConsolidation(final Aggregation aggregation, final List<AggregationChunk> chunksToConsolidate,
	                               CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				startConsolidation(DSL.using(jooqConfiguration), aggregation, chunksToConsolidate);
			}
		}, callback);
	}

	public void startConsolidation(final DSLContext jooq, final Aggregation aggregation,
	                                List<AggregationChunk> chunksToConsolidate) {
//		lockTablesForConsolidation(jooq);

		jooq.update(AGGREGATION_DB_CHUNK)
				.set(AGGREGATION_DB_CHUNK.CONSOLIDATION_STARTED, currentTimestamp())
				.where(AGGREGATION_DB_CHUNK.ID.in(newArrayList(Iterables.transform(chunksToConsolidate, new Function<AggregationChunk, Long>() {
					@Override
					public Long apply(AggregationChunk chunk) {
						return chunk.getChunkId();
					}
				}))))
				.execute();

//		unlockTablesForConsolidation(jooq);
	}

	// TODO - make sure it's working
//	private void lockTablesForConsolidation(DSLContext jooq) {
//		jooq.execute("LOCK TABLES aggregation_db_chunk WRITE");
//	}

//	private void unlockTablesForConsolidation(DSLContext jooq) {
//		jooq.execute("UNLOCK TABLES");
//	}

}
