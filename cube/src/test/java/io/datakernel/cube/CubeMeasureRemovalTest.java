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

import io.datakernel.aggregation_db.AggregationChunk;
import io.datakernel.aggregation_db.AggregationChunkStorage;
import io.datakernel.aggregation_db.LocalFsChunkStorage;
import io.datakernel.aggregation_db.fieldtype.FieldTypes;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogToCubeMetadataStorage;
import io.datakernel.logfs.LogToCubeRunner;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.aggregation_db.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation_db.processor.AggregateFunctions.sum;
import static io.datakernel.cube.Cube.AggregationScheme.id;
import static io.datakernel.cube.CubeTestUtils.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeMeasureRemovalTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String DATABASE_PROPERTIES_PATH = "test.properties";
	private static final SQLDialect DATABASE_DIALECT = SQLDialect.MYSQL;
	private static final String LOG_PARTITION_NAME = "partitionA";
	private static final List<String> LOG_PARTITIONS = asList(LOG_PARTITION_NAME);
	private static final String LOG_NAME = "testlog";

	@Ignore("Requires DB access to run")
	@SuppressWarnings("ConstantConditions")
	@Test
	public void test() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Configuration jooqConfiguration = getJooqConfiguration(DATABASE_PROPERTIES_PATH, DATABASE_DIALECT);
		AggregationChunkStorage aggregationChunkStorage =
				LocalFsChunkStorage.create(eventloop, executor, aggregationsDir);
		CubeMetadataStorageSql cubeMetadataStorageSql =
				CubeMetadataStorageSql.create(eventloop, executor, jooqConfiguration, "processId");
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, cubeMetadataStorageSql);

		Cube cube = Cube.create(eventloop, executor, classLoader, cubeMetadataStorageSql, aggregationChunkStorage)
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

		LogManager<LogItem> logManager = getLogManager(LogItem.class, eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = LogToCubeRunner.create(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(IgnoreCompletionCallback.create());
		eventloop.run();

		cube.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		List<AggregationChunk> chunks = newArrayList(cube.getAggregation("date").getMetadata().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));

		// Initialize cube with new structure (removed measure)
		aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executor, aggregationsDir);

		cube = Cube.create(eventloop, executor, classLoader, cubeMetadataStorageSql, aggregationChunkStorage)
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

		logToCubeRunner = LogToCubeRunner.create(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(IgnoreCompletionCallback.create());
		eventloop.run();

		cube.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		chunks = newArrayList(cube.getAggregation("date").getMetadata().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));
		assertFalse(chunks.get(1).getFields().contains("revenue"));

		chunks = newArrayList(cube.getAggregation("advertiser").getMetadata().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));
		assertTrue(chunks.get(1).getFields().contains("revenue"));

		// Aggregate manually
		HashMap<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);

		StreamConsumers.ToList<LogItem> queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, classLoader).streamTo(queryResultConsumer);
		eventloop.run();
		List<LogItem> queryResultBeforeConsolidation = queryResultConsumer.getList();

		// Check query results
		for (LogItem logItem : queryResultBeforeConsolidation) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}

		// Consolidate
		ResultCallbackFuture<Boolean> callback = ResultCallbackFuture.create();
		cube.consolidate(100, callback);
		eventloop.run();
		boolean consolidated = callback.isDone() ? callback.get() : false;
		assertEquals(true, consolidated);

		cube.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		chunks = newArrayList(cube.getAggregation("date").getMetadata().getChunks().values());
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getFields().contains("revenue"));

		chunks = newArrayList(cube.getAggregation("advertiser").getMetadata().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));

		// Query
		queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, classLoader).streamTo(queryResultConsumer);
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
