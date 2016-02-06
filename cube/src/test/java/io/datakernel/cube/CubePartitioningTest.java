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
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubePartitioningTest {
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
						.build());
	}

	private static Cube getCube(Eventloop eventloop, DefiningClassLoader classLoader,
	                            CubeMetadataStorage cubeMetadataStorage,
	                            AggregationChunkStorage aggregationChunkStorage,
	                            AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, cubeMetadataStorage,
				aggregationChunkStorage, cubeStructure, 1_000_000, 1_000_000);
		cube.addAggregation("date", new AggregationMetadata(asList("date"), LogItem.MEASURES), "date");
		cube.setChildParentRelationships(ImmutableMap.<String, String>builder()
				.put("campaign", "advertiser")
				.put("banner", "campaign")
				.build());
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
	                                                                    CubeMetadataStorageSql aggregationMetadataStorage) {
		CubeMetadataStorageSql cubeMetadataStorage =
				new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration, "processId");
		LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
				jooqConfiguration, aggregationMetadataStorage);
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
		CubeMetadataStorageSql cubeMetadataStorageSql =
				new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration, "processId");
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, cubeMetadataStorageSql);
		Cube cube = getCube(eventloop, classLoader, cubeMetadataStorageSql,
				aggregationChunkStorage, structure);
		LogManager<LogItem> logManager = getLogManager(eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		// Save and aggregate logs
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

		// Load metadata
		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		Map<Long, AggregationChunk> chunks = cube.getAggregations().get("date").getChunks();
		assertEquals(22, chunks.size());

		AggregationQuery query = new AggregationQuery().keys("date").fields("clicks");
		StreamConsumers.ToList<LogItem> queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.query(LogItem.class, query).streamTo(queryResultConsumer);
		eventloop.run();

		// Aggregate manually
		Map<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);

		// Check query results
		for (LogItem logItem : queryResultConsumer.getList()) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}

		int consolidations = 0;
		while (true) {
			cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
			eventloop.run();

			ResultCallbackFuture<Boolean> callback = new ResultCallbackFuture<>();
			cube.consolidate(100, "consolidator", callback);
			eventloop.run();
			boolean consolidated = callback.isDone() ? callback.get() : false;
			if (consolidated)
				++consolidations;
			else
				break;
		}
		assertEquals(11, consolidations);

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

		// Check that every chunk contains only one date
		chunks = cube.getAggregations().get("date").getChunks();
		assertEquals(11, chunks.size());
		for (AggregationChunk chunk : chunks.values()) {
			assertEquals(chunk.getMinPrimaryKey().get(0), chunk.getMaxPrimaryKey().get(0));
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
