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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.FieldTypeDouble;
import io.datakernel.aggregation_db.fieldtype.FieldTypeLong;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeDate;
import io.datakernel.aggregation_db.keytype.KeyTypeInt;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.AsyncExecutors;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.examples.LogItem;
import io.datakernel.examples.LogItemSplitter;
import io.datakernel.logfs.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.newArrayList;
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
			.put("date", new KeyTypeDate())
			.put("advertiser", new KeyTypeInt())
			.put("campaign", new KeyTypeInt())
			.put("banner", new KeyTypeInt())
			.build();

	private static final Map<String, String> CHILD_PARENT_RELATIONSHIPS = ImmutableMap.<String, String>builder()
			.put("campaign", "advertiser")
			.put("banner", "campaign")
			.build();

	private static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				KEYS,
				ImmutableMap.<String, FieldType>builder()
						.put("impressions", new FieldTypeLong())
						.put("clicks", new FieldTypeLong())
						.put("conversions", new FieldTypeLong())
						.put("revenue", new FieldTypeDouble())
						.build(),
				CHILD_PARENT_RELATIONSHIPS);
	}

	private static AggregationStructure getNewStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				KEYS,
				ImmutableMap.<String, FieldType>builder()
						.put("impressions", new FieldTypeLong())
						.put("clicks", new FieldTypeLong())
						.put("conversions", new FieldTypeLong())
						.put("revenue", new FieldTypeDouble())
						.build(),
				CHILD_PARENT_RELATIONSHIPS);
	}

	private static Cube getCube(Eventloop eventloop, DefiningClassLoader classLoader,
	                            CubeMetadataStorage cubeMetadataStorage,
	                            AggregationMetadataStorage aggregationMetadataStorage,
	                            AggregationChunkStorage aggregationChunkStorage,
	                            AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, cubeMetadataStorage, aggregationMetadataStorage,
				aggregationChunkStorage, cubeStructure, 1_000_000, 1_000_000);
		cube.addAggregation(new AggregationMetadata("detailed", LogItem.DIMENSIONS, LogItem.MEASURES));
		cube.addAggregation(new AggregationMetadata("date", asList("date"), LogItem.MEASURES));
		cube.addAggregation(new AggregationMetadata("advertiser", asList("advertiser"), LogItem.MEASURES));
		return cube;
	}

	private static Cube getNewCube(Eventloop eventloop, DefiningClassLoader classLoader,
	                               CubeMetadataStorage cubeMetadataStorage,
	                               AggregationMetadataStorage aggregationMetadataStorage,
	                               AggregationChunkStorage aggregationChunkStorage,
	                               AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, cubeMetadataStorage, aggregationMetadataStorage,
				aggregationChunkStorage, cubeStructure, 1_000_000, 1_000_000);
		cube.addAggregation(new AggregationMetadata("detailed", LogItem.DIMENSIONS,
				asList("impressions", "clicks", "conversions"))); // "revenue" measure is removed
		cube.addAggregation(new AggregationMetadata("date", asList("date"),
				asList("impressions", "clicks", "conversions"))); // "revenue" measure is removed
		cube.addAggregation(new AggregationMetadata("advertiser", asList("advertiser"),
				asList("impressions", "clicks", "conversions", "revenue")));
		return cube;
	}

	private static Configuration getJooqConfiguration() throws IOException {
		Properties properties = new Properties();
		properties.load(new InputStreamReader(
				new BufferedInputStream(new FileInputStream(
						new File(DATABASE_PROPERTIES_PATH))), UTF_8));
		HikariDataSource dataSource = new HikariDataSource(new HikariConfig(properties));

		Configuration jooqConfiguration = new DefaultConfiguration();
		jooqConfiguration.set(new DataSourceConnectionProvider(dataSource));
		jooqConfiguration.set(DATABASE_DIALECT);

		return jooqConfiguration;
	}

	private static LogToCubeMetadataStorage getLogToCubeMetadataStorage(Eventloop eventloop,
	                                                                    ExecutorService executor,
	                                                                    Configuration jooqConfiguration,
	                                                                    AggregationMetadataStorageSql aggregationMetadataStorage) {
		CubeMetadataStorageSql cubeMetadataStorage =
				new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration);
		LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
				jooqConfiguration, cubeMetadataStorage, aggregationMetadataStorage);
		metadataStorage.truncateTables();
		return metadataStorage;
	}

	private static AggregationChunkStorage getAggregationChunkStorage(Eventloop eventloop, ExecutorService executor,
	                                                                  AggregationStructure structure,
	                                                                  Path aggregationsDir) {
		return new LocalFsChunkStorage(eventloop, executor, structure, aggregationsDir);
	}

	private static LogManager<LogItem> getLogManager(Eventloop eventloop, ExecutorService executor,
	                                                 DefiningClassLoader classLoader, Path logsDir) {
		LocalFsLogFileSystem fileSystem = new LocalFsLogFileSystem(eventloop, executor, logsDir);
		BufferSerializer<LogItem> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(LogItem.class);

		return new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);
	}

	@Ignore
	@SuppressWarnings("ConstantConditions")
	@Test
	public void test() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();
		AggregationStructure structure = getStructure(classLoader);

		Configuration jooqConfiguration = getJooqConfiguration();
		AggregationChunkStorage aggregationChunkStorage =
				getAggregationChunkStorage(eventloop, executor, structure, aggregationsDir);
		AggregationMetadataStorageSql aggregationMetadataStorage =
				new AggregationMetadataStorageSql(eventloop, executor, jooqConfiguration);
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, aggregationMetadataStorage);
		Cube cube = getCube(eventloop, classLoader, logToCubeMetadataStorage, aggregationMetadataStorage,
				aggregationChunkStorage, structure);
		LogManager<LogItem> logManager = getLogManager(eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage, LOG_PARTITION_NAME);

		cube.saveAggregations(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();


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
		structure = getNewStructure(classLoader);
		aggregationChunkStorage = getAggregationChunkStorage(eventloop, executor, structure, aggregationsDir);
		cube = getNewCube(eventloop, classLoader, logToCubeMetadataStorage, aggregationMetadataStorage,
				aggregationChunkStorage, structure);
		logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage, LOG_PARTITION_NAME);


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

		AggregationQuery query = new AggregationQuery().keys("date").fields("clicks");
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
		cube.consolidate(100, "consolidator", callback);
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
		query = new AggregationQuery().keys("date").fields("clicks");
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
