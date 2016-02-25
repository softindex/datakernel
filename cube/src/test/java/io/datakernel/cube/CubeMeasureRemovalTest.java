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

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.examples.LogItem;
import io.datakernel.examples.LogItemSplitter;
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
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.doubleSum;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.longSum;
import static io.datakernel.aggregation_db.keytype.KeyTypes.dateKey;
import static io.datakernel.aggregation_db.keytype.KeyTypes.intKey;
import static io.datakernel.cube.CubeTestUtils.*;
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

	private static final Map<String, KeyType> KEYS = ImmutableMap.<String, KeyType>builder()
			.put("date", dateKey())
			.put("advertiser", intKey())
			.put("campaign", intKey())
			.put("banner", intKey())
			.build();

	private static final Map<String, String> CHILD_PARENT_RELATIONSHIPS = ImmutableMap.<String, String>builder()
			.put("campaign", "advertiser")
			.put("banner", "campaign")
			.build();

	private static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				KEYS,
				ImmutableMap.<String, FieldType>builder()
						.put("impressions", longSum())
						.put("clicks", longSum())
						.put("conversions", longSum())
						.put("revenue", doubleSum())
						.build());
	}

	private static Cube getCube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                            CubeMetadataStorage cubeMetadataStorage,
	                            AggregationChunkStorage aggregationChunkStorage,
	                            AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, aggregationChunkStorage,
				cubeStructure, Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY, Aggregation.DEFAULT_SORTER_BLOCK_SIZE,
				Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE);
		cube.addAggregation("detailed", new AggregationMetadata(LogItem.DIMENSIONS, LogItem.MEASURES));
		cube.addAggregation("date", new AggregationMetadata(asList("date"), LogItem.MEASURES));
		cube.addAggregation("advertiser", new AggregationMetadata(asList("advertiser"), LogItem.MEASURES));
		cube.setChildParentRelationships(CHILD_PARENT_RELATIONSHIPS);
		return cube;
	}

	private static Cube getNewCube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                               CubeMetadataStorage cubeMetadataStorage,
	                               AggregationChunkStorage aggregationChunkStorage,
	                               AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, aggregationChunkStorage,
				cubeStructure, Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY, Aggregation.DEFAULT_SORTER_BLOCK_SIZE,
				Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE);
		cube.addAggregation("detailed", new AggregationMetadata(LogItem.DIMENSIONS,
				asList("impressions", "clicks", "conversions"))); // "revenue" measure is removed
		cube.addAggregation("date", new AggregationMetadata(asList("date"),
				asList("impressions", "clicks", "conversions"))); // "revenue" measure is removed
		cube.addAggregation("advertiser", new AggregationMetadata(asList("advertiser"),
				asList("impressions", "clicks", "conversions", "revenue")));
		cube.setChildParentRelationships(CHILD_PARENT_RELATIONSHIPS);
		return cube;
	}

	@Ignore("Requires DB access to run")
	@SuppressWarnings("ConstantConditions")
	@Test
	public void test() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();
		AggregationStructure structure = getStructure(classLoader);

		Configuration jooqConfiguration = getJooqConfiguration(DATABASE_PROPERTIES_PATH, DATABASE_DIALECT);
		AggregationChunkStorage aggregationChunkStorage =
				getAggregationChunkStorage(eventloop, executor, structure, aggregationsDir);
		CubeMetadataStorageSql cubeMetadataStorageSql =
				new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration, "processId");
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, cubeMetadataStorageSql);
		Cube cube = getCube(eventloop, executor, classLoader, cubeMetadataStorageSql,
				aggregationChunkStorage, structure);
		LogManager<LogItem> logManager = getLogManager(LogItem.class, eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		List<AggregationChunk> chunks = newArrayList(cube.getAggregations().get("date").getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));


		// Initialize cube with new structure (removed measure)
		structure = getStructure(classLoader);
		aggregationChunkStorage = getAggregationChunkStorage(eventloop, executor, structure, aggregationsDir);
		cube = getNewCube(eventloop, executor, classLoader, cubeMetadataStorageSql, aggregationChunkStorage, structure);
		logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);


		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		chunks = newArrayList(cube.getAggregations().get("date").getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));
		assertFalse(chunks.get(1).getFields().contains("revenue"));

		chunks = newArrayList(cube.getAggregations().get("advertiser").getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));
		assertTrue(chunks.get(1).getFields().contains("revenue"));


		// Aggregate manually
		HashMap<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);

		CubeQuery query = new CubeQuery().dimensions("date").measures("clicks");
		StreamConsumers.ToList<LogItem> queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.query(LogItem.class, query).streamTo(queryResultConsumer);
		eventloop.run();
		List<LogItem> queryResultBeforeConsolidation = queryResultConsumer.getList();


		// Check query results
		for (LogItem logItem : queryResultBeforeConsolidation) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}


		// Consolidate
		ResultCallbackFuture<Boolean> callback = new ResultCallbackFuture<>();
		cube.consolidate(100, callback);
		eventloop.run();
		boolean consolidated = callback.isDone() ? callback.get() : false;
		assertEquals(true, consolidated);

		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		chunks = newArrayList(cube.getAggregations().get("date").getChunks().values());
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getFields().contains("revenue"));

		chunks = newArrayList(cube.getAggregations().get("advertiser").getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getFields().contains("revenue"));


		// Query
		query = new CubeQuery().dimensions("date").measures("clicks");
		queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.query(LogItem.class, query).streamTo(queryResultConsumer);
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
