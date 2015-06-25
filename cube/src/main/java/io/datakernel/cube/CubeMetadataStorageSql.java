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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Longs;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.sql.tables.records.AggregationChunkRecord;
import io.datakernel.cube.sql.tables.records.AggregationStructureRecord;
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
import static io.datakernel.async.AsyncCallbacks.callConcurrently;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static io.datakernel.cube.AggregationChunk.createCommitChunk;
import static io.datakernel.cube.AggregationChunk.createConsolidateChunk;
import static io.datakernel.cube.sql.tables.AggregationChunk.AGGREGATION_CHUNK;
import static io.datakernel.cube.sql.tables.AggregationRevision.AGGREGATION_REVISION;
import static io.datakernel.cube.sql.tables.AggregationStructure.AGGREGATION_STRUCTURE;
import static org.jooq.impl.DSL.*;

/**
 * Stores cube metadata in relational database.
 */
public final class CubeMetadataStorageSql implements CubeMetadataStorage {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;

	/**
	 * Constructs a cube metadata storage, that runs in the specified event loop, performs SQL queries in the given executor,
	 * and connects to RDBMS using the specified configuration.
	 *
	 * @param eventloop         event loop, in which metadata storage is to run
	 * @param executor          executor, where SQL queries are to be run
	 * @param jooqConfiguration database connection configuration
	 */
	public CubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
	}

	public void truncateTables(DSLContext jooq) {
		jooq.truncate(AGGREGATION_CHUNK).execute();
		jooq.truncate(AGGREGATION_STRUCTURE).execute();
		jooq.truncate(AGGREGATION_REVISION).execute();
	}

	public void truncateTables() {
		truncateTables(DSL.using(jooqConfiguration));
	}

	public int nextRevisionId(DSLContext jooq) {
		return jooq
				.insertInto(AGGREGATION_REVISION)
				.defaultValues()
				.returning(AGGREGATION_REVISION.ID)
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
				.insertInto(AGGREGATION_CHUNK, AGGREGATION_CHUNK.REVISION_ID, AGGREGATION_CHUNK.MIN_REVISION_ID,
						AGGREGATION_CHUNK.MAX_REVISION_ID, AGGREGATION_CHUNK.COUNT)
				.values(0, 0, 0, 0)
				.returning(AGGREGATION_CHUNK.ID)
				.fetchOne()
				.getId();
	}

	public void doSaveChunks(DSLContext jooq, Multimap<Aggregation, AggregationChunk> chunks) {
		for (Aggregation aggregation : chunks.keySet()) {
			saveChunks(jooq, aggregation, chunks.get(aggregation));
		}
	}

	private void saveChunks(DSLContext jooq, Aggregation aggregation, Collection<AggregationChunk> chunks) {
		for (AggregationChunk chunk : chunks) {
			AggregationChunkRecord record = new AggregationChunkRecord();
			record.setId(chunk.getChunkId());
			record.setAggregationId(aggregation.getId());
			record.setRevisionId(chunk.getRevisionId());
			record.setMinRevisionId(chunk.getMinRevisionId());
			record.setMaxRevisionId(chunk.getMaxRevisionId());
			record.setDimensions(Joiner.on(' ').join(aggregation.getDimensions()));
			record.setMetrics(Joiner.on(' ').join(chunk.getMeasures()));
			record.setCount(chunk.getCount());
			record.setCreated(new Timestamp(Calendar.getInstance().getTime().getTime()));

			Map<Field<?>, Object> map = new LinkedHashMap<>();
			int size = record.size();
			for (int i = 1; i < size; i++) {
				map.put(record.field(i), record.getValue(i));
			}

			for (int d = 0; d < aggregation.getDimensions().size(); d++) {
				map.put(field("d" + (d + 1) + "_min"), chunk.getMinPrimaryKey().values().get(d));
				map.put(field("d" + (d + 1) + "_max"), chunk.getMaxPrimaryKey().values().get(d));
			}

			jooq.update(AGGREGATION_CHUNK).set(map).where(AGGREGATION_CHUNK.ID.equal(chunk.getChunkId())).execute();
		}
	}

	@Override
	public void saveChunks(final Aggregation aggregation, final List<AggregationChunk.NewChunk> newChunks, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				DSLContext jooq = DSL.using(jooqConfiguration);

				final int revisionId = nextRevisionId(jooq);

				saveChunks(jooq, aggregation, transform(newChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
					@Override
					public AggregationChunk apply(AggregationChunk.NewChunk newChunk) {
						return createCommitChunk(revisionId, newChunk);
					}
				}));
			}
		}, callback);
	}

	public void saveChunks(DSLContext jooq, Multimap<Aggregation, AggregationChunk.NewChunk> newChunks) {
		final int revisionId = nextRevisionId(jooq);

		doSaveChunks(jooq, transformValues(newChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
			@Override
			public AggregationChunk apply(AggregationChunk.NewChunk newChunk) {
				return createCommitChunk(revisionId, newChunk);
			}
		}));
	}

	public void loadAggregations(DSLContext jooq,
	                             Cube cube) {
		Result<AggregationStructureRecord> records = jooq
				.selectFrom(AGGREGATION_STRUCTURE)
				.where(AGGREGATION_STRUCTURE.ID.notIn(cube.getAggregations().keySet()))
				.fetch();

		Splitter splitter = Splitter.on(' ').omitEmptyStrings();
		for (AggregationStructureRecord record : records) {
			Aggregation aggregation = new Aggregation(record.getId(),
					newArrayList(splitter.split(record.getDimensions())),
					newArrayList(splitter.split(record.getMetrics())));
			cube.addAggregation(aggregation);
		}
	}

	@Override
	public void loadAggregations(final Cube cube, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				loadAggregations(DSL.using(jooqConfiguration), cube);
			}
		}, callback);
	}

	private void saveAggregations(DSLContext jooq,
	                              Cube cube) {
		Joiner joiner = Joiner.on(' ');
		for (Aggregation aggregation : cube.getAggregations().values()) {
			jooq.insertInto(AGGREGATION_STRUCTURE)
					.set(new AggregationStructureRecord(
							aggregation.getId(),
							joiner.join(aggregation.getDimensions()),
							joiner.join(aggregation.getMeasures())))
					.onDuplicateKeyIgnore()
					.execute();
		}
	}

	@Override
	public void saveAggregations(final Cube cube, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				saveAggregations(DSL.using(jooqConfiguration), cube);
			}
		}, callback);
	}

	public int loadChunks(DSLContext jooq, Cube cube, int lastRevisionId, int maxRevisionId) {
		Record1<Integer> maxRevisionRecord = jooq
				.select(DSL.max(AGGREGATION_REVISION.ID))
				.from(AGGREGATION_REVISION)
				.where(AGGREGATION_REVISION.ID.gt(lastRevisionId)
						.and(AGGREGATION_REVISION.ID.le(maxRevisionId)))
				.fetchOne();
		if (maxRevisionRecord.value1() == null)
			return lastRevisionId;
		int newRevisionId = maxRevisionRecord.value1();

		Multimap<Integer, Long> sourceChunkIdsMultimap = LinkedListMultimap.create();
		Map<Integer, long[]> sourceChunkIdsMap = new HashMap<>();

		if (lastRevisionId != 0) {
			Result<Record4<Long, Integer, Timestamp, Timestamp>> consolidatedChunksRecords = jooq
					.select(AGGREGATION_CHUNK.ID, AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID,
							AGGREGATION_CHUNK.CONSOLIDATION_STARTED, AGGREGATION_CHUNK.CONSOLIDATION_COMPLETED)
					.from(AGGREGATION_CHUNK)
					.where(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID.gt(lastRevisionId))
					.and(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID.le(newRevisionId))
					.fetch();

			for (Record4<Long, Integer, Timestamp, Timestamp> record : consolidatedChunksRecords) {
				Integer consolidatedRevisionId = record.getValue(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID);
				Long sourceChunkId = record.getValue(AGGREGATION_CHUNK.ID);
				Timestamp consolidationStarted = record.getValue(AGGREGATION_CHUNK.CONSOLIDATION_STARTED);
				Timestamp consolidationCompleted = record.getValue(AGGREGATION_CHUNK.CONSOLIDATION_COMPLETED);
				sourceChunkIdsMultimap.put(consolidatedRevisionId, sourceChunkId);

				AggregationChunk chunk = cube.getChunks().get(sourceChunkId);

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
				.from(AGGREGATION_CHUNK.getName())
				.where(AGGREGATION_CHUNK.REVISION_ID.gt(lastRevisionId))
				.and(AGGREGATION_CHUNK.REVISION_ID.le(newRevisionId))
				.and(lastRevisionId == 0
						? AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID.isNull().or(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID.gt(newRevisionId))
						: trueCondition())
				.fetch();

		Splitter splitter = Splitter.on(' ').omitEmptyStrings();
		for (Record record : newChunkRecords) {
			Aggregation aggregation = cube.getAggregations().get(record.getValue(AGGREGATION_CHUNK.AGGREGATION_ID));
			boolean isConsolidated = record.getValue(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID) != null &&
					record.getValue(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID) <= newRevisionId;
//			LinkedHashSet<String> dimensions = newLinkedHashSet(splitter.split(record.getValue(AGGREGATION_CHUNK.DIMENSIONS)));
			Object[] minKeyArray = new Object[aggregation.getDimensions().size()];
			Object[] maxKeyArray = new Object[aggregation.getDimensions().size()];
			for (int d = 0; d < aggregation.getDimensions().size(); d++) {
				String dimension = aggregation.getDimensions().get(d);
				Class<?> type = cube.getStructure().getDimensions().get(dimension).getDataType();
				minKeyArray[d] = record.getValue("d" + (d + 1) + "_min", type);
				maxKeyArray[d] = record.getValue("d" + (d + 1) + "_max", type);
			}
			AggregationChunk chunk = new AggregationChunk(record.getValue(AGGREGATION_CHUNK.REVISION_ID),
					record.getValue(AGGREGATION_CHUNK.ID).intValue(),
					record.getValue(AGGREGATION_CHUNK.AGGREGATION_ID),
					record.getValue(AGGREGATION_CHUNK.MIN_REVISION_ID),
					record.getValue(AGGREGATION_CHUNK.MAX_REVISION_ID),
					sourceChunkIdsMap.get(record.getValue(AGGREGATION_CHUNK.REVISION_ID)),
					newArrayList(splitter.split(record.getValue(AGGREGATION_CHUNK.METRICS))),
					PrimaryKey.ofArray(minKeyArray),
					PrimaryKey.ofArray(maxKeyArray),
					record.getValue(AGGREGATION_CHUNK.COUNT));
			cube.addChunk(chunk);
			if (!isConsolidated) {
				aggregation.addToIndex(chunk);
			}
		}

		for (Long chunkId : sourceChunkIdsMultimap.values()) {
			AggregationChunk chunk = cube.getChunks().get(chunkId);
			Aggregation aggregation = cube.getAggregations().get(chunk.getAggregationId());
			aggregation.removeFromIndex(chunk);
		}

		return newRevisionId;
	}

	@Override
	public void loadChunks(final Cube cube,
	                       final int lastRevisionId, final int maxRevisionId,
	                       ResultCallback<Integer> callback) {
		callConcurrently(eventloop, executor, false, new Callable<Integer>() {
			@Override
			public Integer call() {
				return loadChunks(DSL.using(jooqConfiguration), cube, lastRevisionId, maxRevisionId);
			}
		}, callback);
	}

	@Override
	public void reloadAllChunkConsolidations(final Cube cube, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				reloadAllChunkConsolidations(using(jooqConfiguration), cube);
			}
		}, callback);
	}

	private void reloadAllChunkConsolidations(DSLContext jooq, Cube cube) {
		Result<Record4<Long, Integer, Timestamp, Timestamp>> chunkRecords = jooq
				.select(AGGREGATION_CHUNK.ID, AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID,
						AGGREGATION_CHUNK.CONSOLIDATION_STARTED, AGGREGATION_CHUNK.CONSOLIDATION_COMPLETED)
				.from(AGGREGATION_CHUNK)
				.fetch();

		for (Record4<Long, Integer, Timestamp, Timestamp> chunkRecord : chunkRecords) {
			AggregationChunk chunk = cube.getChunks().get(chunkRecord.getValue(AGGREGATION_CHUNK.ID));
			if (chunk != null) {
				chunk.setConsolidatedRevisionId(chunkRecord.getValue(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID));
				chunk.setConsolidationStarted(chunkRecord.getValue(AGGREGATION_CHUNK.CONSOLIDATION_STARTED));
				chunk.setConsolidationCompleted(chunkRecord.getValue(AGGREGATION_CHUNK.CONSOLIDATION_COMPLETED));
			}
		}
	}

	@Override
	public void performConsolidation(final Cube cube,
	                                 final Function<List<Long>, List<AggregationChunk>> chunkConsolidator,
	                                 CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				performConsolidation(DSL.using(jooqConfiguration), cube, chunkConsolidator);
			}
		}, callback);
	}

	private void performConsolidation(final DSLContext jooq, final Cube cube,
	                                  final Function<List<Long>, List<AggregationChunk>> chunkConsolidator) {
		lockTablesForConsolidation(jooq);

		refreshChunkConsolidations(jooq, cube);

		List<AggregationChunk> chunksToConsolidate = chunkConsolidator.apply(cube.getIdsOfChunksAvailableForConsolidation());

		if (chunksToConsolidate != null && chunksToConsolidate.size() > 0) {
			writeConsolidationStartTimestamp(jooq, chunksToConsolidate);
		}

		unlockTablesForConsolidation(jooq);
	}

	private void lockTablesForConsolidation(DSLContext jooq) {
		jooq.execute("LOCK TABLES aggregation_chunk WRITE");
	}

	private void unlockTablesForConsolidation(DSLContext jooq) {
		jooq.execute("UNLOCK TABLES");
	}

	private void writeConsolidationStartTimestamp(DSLContext jooq, List<AggregationChunk> chunks) {
		jooq.update(AGGREGATION_CHUNK)
				.set(AGGREGATION_CHUNK.CONSOLIDATION_STARTED, currentTimestamp())
				.where(AGGREGATION_CHUNK.ID.in(newArrayList(Iterables.transform(chunks, new Function<AggregationChunk, Long>() {
					@Override
					public Long apply(AggregationChunk chunk) {
						return chunk.getChunkId();
					}
				}))))
				.execute();
	}

	@Override
	public void refreshNotConsolidatedChunks(final Cube cube, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				refreshChunkConsolidations(DSL.using(jooqConfiguration), cube);
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
		jooq.delete(AGGREGATION_CHUNK).where(AGGREGATION_CHUNK.ID.eq(chunkId)).execute();
	}

	private void refreshChunkConsolidations(DSLContext jooq, Cube cube) {
		Result<Record3<Long, Integer, Timestamp>> chunkRecords = jooq
				.select(AGGREGATION_CHUNK.ID, AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID, AGGREGATION_CHUNK.CONSOLIDATION_STARTED)
				.from(AGGREGATION_CHUNK)
				.where(AGGREGATION_CHUNK.ID.in(cube.getIdsOfNotConsolidatedChunks()))
				.fetch();

		for (Record3<Long, Integer, Timestamp> chunkRecord : chunkRecords) {
			AggregationChunk chunk = cube.getChunks().get(chunkRecord.getValue(AGGREGATION_CHUNK.ID));
			if (chunk != null) {
				chunk.setConsolidatedRevisionId(chunkRecord.getValue(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID));
				chunk.setConsolidationStarted(chunkRecord.getValue(AGGREGATION_CHUNK.CONSOLIDATION_STARTED));
			}
		}
	}

	public void saveConsolidatedChunks(DSLContext jooq, final Cube cube,
	                                   final Aggregation aggregation,
	                                   final List<AggregationChunk> originalChunks,
	                                   final List<AggregationChunk.NewChunk> consolidatedChunks) {
		jooq.transaction(new TransactionalRunnable() {
			@Override
			public void run(Configuration jooqConfiguration) throws Exception {
				DSLContext jooq = DSL.using(jooqConfiguration);

				final int revisionId = nextRevisionId(jooq);

				jooq.update(AGGREGATION_CHUNK)
						.set(AGGREGATION_CHUNK.CONSOLIDATED_REVISION_ID, revisionId)
						.set(AGGREGATION_CHUNK.CONSOLIDATION_COMPLETED, currentTimestamp())
						.where(AGGREGATION_CHUNK.ID.in(newArrayList(Iterables.transform(originalChunks, new Function<AggregationChunk, Long>() {
							@Override
							public Long apply(AggregationChunk chunk) {
								return chunk.getChunkId();
							}
						}))))
						.execute();

				ArrayListMultimap<Aggregation, AggregationChunk> multimap = ArrayListMultimap.create();
				multimap.putAll(aggregation, Iterables.transform(consolidatedChunks, new Function<AggregationChunk.NewChunk, AggregationChunk>() {
					@Override
					public AggregationChunk apply(AggregationChunk.NewChunk newChunk) {
						return createConsolidateChunk(revisionId, originalChunks, newChunk);
					}
				}));
				doSaveChunks(jooq, multimap);
			}
		});

		loadAggregations(jooq, cube);
		cube.setLastRevisionId(loadChunks(jooq, cube, cube.getLastRevisionId(), Integer.MAX_VALUE));
	}

	@Override
	public void saveConsolidatedChunks(final Cube cube, final Aggregation aggregation,
	                                   final List<AggregationChunk> originalChunks,
	                                   final List<AggregationChunk.NewChunk> consolidatedChunks,
	                                   CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				saveConsolidatedChunks(DSL.using(jooqConfiguration), cube, aggregation, originalChunks, consolidatedChunks);
			}
		}, callback);
	}

}
