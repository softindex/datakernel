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
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.NioEventloop;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeIntegrationTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String DATABASE_PROPERTIES_PATH = "test.properties";
	private static final SQLDialect DATABASE_DIALECT = SQLDialect.MYSQL;
	private static final String LOG_PARTITION_NAME = "partitionA";
	private static final List<String> LOG_PARTITIONS = asList(LOG_PARTITION_NAME);
	private static final String LOG_NAME = "testlog";

	private static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("date", new KeyTypeDate())
						.put("advertiser", new KeyTypeInt())
						.put("campaign", new KeyTypeInt())
						.put("banner", new KeyTypeInt())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("impressions", new FieldTypeLong())
						.put("clicks", new FieldTypeLong())
						.put("conversions", new FieldTypeLong())
						.put("revenue", new FieldTypeDouble())
						.build(),
				ImmutableMap.<String, String>builder()
						.put("campaign", "advertiser")
						.put("banner", "campaign")
						.build());
	}

	private static Cube getCube(NioEventloop eventloop, DefiningClassLoader classLoader,
	                            CubeMetadataStorage cubeMetadataStorage,
	                            AggregationMetadataStorage aggregationMetadataStorage,
	                            AggregationChunkStorage aggregationChunkStorage,
	                            AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, cubeMetadataStorage, aggregationMetadataStorage, aggregationChunkStorage, cubeStructure, 1_000_000, 1_000_000);
		cube.addAggregation(new AggregationMetadata("detailed", LogItem.DIMENSIONS, LogItem.MEASURES));
		cube.addAggregation(new AggregationMetadata("date", asList("date"), LogItem.MEASURES));
		cube.addAggregation(new AggregationMetadata("advertiser", asList("advertiser"), LogItem.MEASURES));
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

	private static LogToCubeMetadataStorage getLogToCubeMetadataStorage(NioEventloop eventloop,
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

	private static AggregationChunkStorage getAggregationChunkStorage(NioEventloop eventloop, ExecutorService executor,
	                                                                  AggregationStructure structure, Path aggregationsDir) {
		return new LocalFsChunkStorage(eventloop, executor, structure, aggregationsDir);
	}

	private static LogManager<LogItem> getLogManager(NioEventloop eventloop, ExecutorService executor,
	                                                 DefiningClassLoader classLoader, Path logsDir) {
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, logsDir);
		BufferSerializer<LogItem> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(LogItem.class);

		return new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);
	}

	private static StreamProducers.OfIterator<LogItem> getProducerOfRandomLogItems(NioEventloop eventloop, int numberOfTestItems) {
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(numberOfTestItems);
		return new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems.iterator());
	}

	@Ignore
	@Test
	public void test() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = new DefiningClassLoader();
		NioEventloop eventloop = new NioEventloop();
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
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		cube.saveAggregations(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();


		// Save logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(300);
		producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		List<LogItem> listOfRandomLogItems3 = LogItem.getListOfRandomLogItems(50);
		producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems3.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();


		// Aggregate logs
		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();


		// Load metadata
		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();


		AggregationQuery query = new AggregationQuery().keys("date").fields("clicks");
		StreamConsumers.ToList<LogItem> queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.query(LogItem.class, query).streamTo(queryResultConsumer);
		eventloop.run();


		// Aggregate manually
		HashMap<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);
		aggregateToMap(map, listOfRandomLogItems3);


		// Check query results
		for (LogItem logItem : queryResultConsumer.getList()) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}


		// Consolidate
		ResultCallbackFuture<Boolean> callback = new ResultCallbackFuture<>();
		cube.consolidate(100, callback);
		eventloop.run();
		boolean consolidated = callback.isDone() ? callback.get() : false;
		assertEquals(true, consolidated);


		// Load metadata
		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();


		// Query
		queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.query(LogItem.class, query).streamTo(queryResultConsumer);
		eventloop.run();


		// Check query results
		for (LogItem logItem : queryResultConsumer.getList()) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}


		// Check files in aggregations directory
		Set<String> actualChunkFileNames = new TreeSet<>();
		for (File file : aggregationsDir.toFile().listFiles()) {
			actualChunkFileNames.add(file.getName());
		}
		Set<String> expectedChunkFileNames = new TreeSet<>();
		for (int i = 1; i <= 12; ++i) {
			expectedChunkFileNames.add(i + ".log");
		}
		assertEquals(expectedChunkFileNames, actualChunkFileNames);
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
