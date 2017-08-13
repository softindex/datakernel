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

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeDiffJson;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.LocalFsLogFileSystem;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogManagerImpl;
import io.datakernel.logfs.ot.*;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteAdapter;
import io.datakernel.ot.OTRemoteSql;
import io.datakernel.ot.OTStateManager;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.CubeTestUtils.dataSource;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeMeasureRemovalTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@SuppressWarnings("ConstantConditions")
	@Test
	public void test() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		AggregationChunkStorage aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executor, new IdGeneratorStub(), aggregationsDir);
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withDimension("advertiser", FieldTypes.ofInt())
				.withDimension("campaign", FieldTypes.ofInt())
				.withDimension("banner", FieldTypes.ofInt())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()))
				.withAggregation(id("detailed")
						.withDimensions("date", "advertiser", "campaign", "banner")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("advertiser")
						.withDimensions("advertiser")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign");

		DataSource dataSource = dataSource("test.properties");
		OTRemoteSql<LogDiff<CubeDiff>> otSourceSql = OTRemoteSql.create(dataSource, LogDiffJson.create(CubeDiffJson.create(cube)));
		otSourceSql.truncateTables();
		otSourceSql.push(OTCommit.<Integer, LogDiff<CubeDiff>>ofRoot(1));

		LogManager<LogItem> logManager = LogManagerImpl.create(eventloop,
				LocalFsLogFileSystem.create(eventloop, executor, logsDir),
				SerializerBuilder.create(classLoader).build(LogItem.class));

		OTStateManager<Integer, LogDiff<CubeDiff>> logCubeStateManager = new OTStateManager<>(eventloop,
				LogOT.createLogOT(CubeOT.createCubeOT()),
				OTRemoteAdapter.ofBlockingRemote(eventloop, executor, otSourceSql),
				new Comparator<Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return Integer.compare(o1, o2);
					}
				},
				new LogOTState<>(cube));

		LogOTProcessor<Integer, LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				logManager,
				cube.logStreamConsumer(LogItem.class),
				"testlog",
				asList("partitionA"),
				logCubeStateManager);

		CompletionCallbackFuture future;

		// checkout first (root) revision

		future = CompletionCallbackFuture.create();
		logCubeStateManager.checkout(future);
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		StreamProducer<LogItem> producerOfRandomLogItems = StreamProducers.ofIterator(eventloop, listOfRandomLogItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer("partitionA"));
		eventloop.run();

		future = CompletionCallbackFuture.create();
		logOTProcessor.processLog(future);
		eventloop.run();
		future.get();

		List<AggregationChunk> chunks = newArrayList(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Initialize cube with new structure (removed measure)
		cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withDimension("advertiser", FieldTypes.ofInt())
				.withDimension("campaign", FieldTypes.ofInt())
				.withDimension("banner", FieldTypes.ofInt())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()))
				.withAggregation(id("detailed")
						.withDimensions("date", "advertiser", "campaign", "banner")
						.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
				.withAggregation(id("advertiser")
						.withDimensions("advertiser")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign");

		logCubeStateManager = new OTStateManager<>(eventloop,
				LogOT.createLogOT(CubeOT.createCubeOT()),
				OTRemoteAdapter.ofBlockingRemote(eventloop, executor, otSourceSql),
				new Comparator<Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return Integer.compare(o1, o2);
					}
				},
				new LogOTState<>(cube));

		logOTProcessor = LogOTProcessor.create(eventloop,
				logManager,
				cube.logStreamConsumer(LogItem.class),
				"testlog",
				asList("partitionA"),
				logCubeStateManager);

		future = CompletionCallbackFuture.create();
		logCubeStateManager.checkout(future);
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		producerOfRandomLogItems = StreamProducers.ofIterator(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer("partitionA"));
		eventloop.run();

		future = CompletionCallbackFuture.create();
		logOTProcessor.processLog(future);
		eventloop.run();
		future.get();

		chunks = newArrayList(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertFalse(chunks.get(1).getMeasures().contains("revenue"));

		chunks = newArrayList(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertTrue(chunks.get(1).getMeasures().contains("revenue"));

		// Aggregate manually
		HashMap<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);

		StreamConsumers.ToList<LogItem> queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer);
		eventloop.run();
		List<LogItem> queryResultBeforeConsolidation = queryResultConsumer.getList();

		// Check query results
		for (LogItem logItem : queryResultBeforeConsolidation) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}

		// Consolidate
		ResultCallbackFuture<CubeDiff> callback = ResultCallbackFuture.create();
		cube.consolidate(callback);
		eventloop.run();
		CubeDiff consolidatingCubeDiff = callback.get();
		assertEquals(false, consolidatingCubeDiff.isEmpty());

		future = CompletionCallbackFuture.create();
		logCubeStateManager.apply(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		logCubeStateManager.commitAndPush(future);
		eventloop.run();

		chunks = newArrayList(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getMeasures().contains("revenue"));

		chunks = newArrayList(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Query
		queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer);
		eventloop.run();
		List<LogItem> queryResultAfterConsolidation = queryResultConsumer.getList();

		// Check that query results before and after consolidation match
		assertEquals(queryResultBeforeConsolidation.size(), queryResultAfterConsolidation.size());
		for (int i = 0; i < queryResultBeforeConsolidation.size(); ++i) {
			assertEquals(queryResultBeforeConsolidation.get(i).date, queryResultAfterConsolidation.get(i).date);
			assertEquals(queryResultBeforeConsolidation.get(i).clicks, queryResultAfterConsolidation.get(i).clicks);
		}
	}

	private void aggregateToMap(Map<Integer, Long> map, List<LogItem> logItems) {
		for (LogItem logItem : logItems) {
			int date = logItem.date;
			long clicks = logItem.clicks;
			if (map.get(date) == null) {
				map.put(date, clicks);
			} else {
				Long clicksForDate = map.get(date);
				map.put(date, clicksForDate + clicks);
			}
		}
	}
}
