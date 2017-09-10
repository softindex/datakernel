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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
		OTRemoteSql<LogDiff<CubeDiff>> otSourceSql = OTRemoteSql.create(executor, dataSource, LogDiffJson.create(CubeDiffJson.create(cube)));
		otSourceSql.truncateTables();
		otSourceSql.push(OTCommit.ofRoot(1));

		LogManager<LogItem> logManager = LogManagerImpl.create(eventloop,
				LocalFsLogFileSystem.create(executor, logsDir),
				SerializerBuilder.create(classLoader).build(LogItem.class));

		OTStateManager<Integer, LogDiff<CubeDiff>> logCubeStateManager = new OTStateManager<>(eventloop,
				LogOT.createLogOT(CubeOT.createCubeOT()),
				otSourceSql,
				(o1, o2) -> Integer.compare(o1, o2),
				new LogOTState<>(cube));

		LogOTProcessor<Integer, LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				logManager,
				cube.logStreamConsumer(LogItem.class),
				"testlog",
				asList("partitionA"),
				logCubeStateManager);

		// checkout first (root) revision

		CompletableFuture<?> future;

		future = logCubeStateManager.checkout().toCompletableFuture();
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems1 = LogItem.getListOfRandomLogItems(100);
		StreamProducer<LogItem> producerOfRandomLogItems1 = StreamProducers.ofIterator(eventloop, listOfRandomLogItems1.iterator());
		producerOfRandomLogItems1.streamTo(logManager.consumerStream("partitionA"));
		eventloop.run();

		future = logOTProcessor.processLog().toCompletableFuture();
		eventloop.run();
		future.get();

		List<AggregationChunk> chunks = newArrayList(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		StreamConsumers.ToList<LogItem> queryResultConsumer1 = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer1);
		eventloop.run();

		HashMap<Integer, Long> map1 = new HashMap<>();
		aggregateToMap(map1, listOfRandomLogItems1);

		List<LogItem> queryResult1 = queryResultConsumer1.getList();
		for (LogItem logItem : queryResult1) {
			assertEquals(logItem.clicks, map1.get(logItem.date).longValue());
		}

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
				otSourceSql,
				(o1, o2) -> Integer.compare(o1, o2),
				new LogOTState<>(cube));

		logOTProcessor = LogOTProcessor.create(eventloop,
				logManager,
				cube.logStreamConsumer(LogItem.class),
				"testlog",
				asList("partitionA"),
				logCubeStateManager);

		future = logCubeStateManager.checkout().toCompletableFuture();
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		StreamProducer<LogItem> producerOfRandomLogItems2 = StreamProducers.ofIterator(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems2.streamTo(logManager.consumerStream("partitionA"));
		eventloop.run();

		future = logOTProcessor.processLog().toCompletableFuture();
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
		HashMap<Integer, Long> map2 = new HashMap<>();
		aggregateToMap(map2, listOfRandomLogItems1);
		aggregateToMap(map2, listOfRandomLogItems2);

		StreamConsumers.ToList<LogItem> queryResultConsumer2 = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer2);
		eventloop.run();

		// Check query results
		List<LogItem> queryResult2 = queryResultConsumer2.getList();
		for (LogItem logItem : queryResult2) {
			assertEquals(logItem.clicks, map2.get(logItem.date).longValue());
		}

		// Consolidate
		CompletableFuture<CubeDiff> future1 = cube.consolidate().toCompletableFuture();
		eventloop.run();
		CubeDiff consolidatingCubeDiff = future1.get();
		assertEquals(false, consolidatingCubeDiff.isEmpty());

		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		future = logCubeStateManager.commitAndPush().toCompletableFuture();
		eventloop.run();
		future.get();

		chunks = newArrayList(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getMeasures().contains("revenue"));

		chunks = newArrayList(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Query
		StreamConsumers.ToList<LogItem> queryResultConsumer3 = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer3);
		eventloop.run();
		List<LogItem> queryResult3 = queryResultConsumer3.getList();

		// Check that query results before and after consolidation match
		assertEquals(queryResult2.size(), queryResult3.size());
		for (int i = 0; i < queryResult2.size(); ++i) {
			assertEquals(queryResult2.get(i).date, queryResult3.get(i).date);
			assertEquals(queryResult2.get(i).clicks, queryResult3.get(i).clicks);
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
