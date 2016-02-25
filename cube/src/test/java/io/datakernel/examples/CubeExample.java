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

package io.datakernel.examples;

import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeMetadataStorage;
import io.datakernel.cube.CubeMetadataStorageSql;
import io.datakernel.cube.api.CubeHttpServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.logfs.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamProducers;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Charsets.UTF_8;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.doubleSum;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.longSum;
import static io.datakernel.aggregation_db.keytype.KeyTypes.dateKey;
import static io.datakernel.aggregation_db.keytype.KeyTypes.intKey;
import static io.datakernel.cube.TestUtils.deleteRecursivelyQuietly;
import static java.util.Arrays.asList;

/**
 * Example, that describes the essential steps to get the cube up and running, along with log processing tools
 * and HTTP server, that allows users to perform JSON queries.
 * To run this example, do the following:
 * 1. Create *.properties file with database access configuration (driver, URL, username, password)
 * and specify the path to this file in DATABASE_PROPERTIES_PATH constant
 * (it is advised to launch examples in $MODULE_DIR$, so paths are resolved properly).
 * 2. Specify SQLDialect in DATABASE_DIALECT constant according to RDBMS you are using.
 * 3. (Optional) Change directories (AGGREGATIONS_PATH and LOGS_PATH) used to store test data.
 * 4. (Optional) Change names of log partitions (LOG_PARTITION_NAME and LOG_PARTITIONS).
 * 5. (Optional) Define another port to be used by HTTP server (HTTP_SERVER_PORT).
 * 6. (Optional) Adjust the size of test data set (NUMBER_OF_TEST_ITEMS).
 * 7. Run main() and enjoy.
 */
public class CubeExample {
	private static final String DATABASE_PROPERTIES_PATH = "test.properties";
	private static final SQLDialect DATABASE_DIALECT = SQLDialect.MYSQL;
	private static final String AGGREGATIONS_PATH = "test/aggregations/";
	private static final String LOGS_PATH = "test/logs/";
	private static final String LOG_PARTITION_NAME = "partitionA";
	private static final List<String> LOG_PARTITIONS = asList(LOG_PARTITION_NAME);
	private static final String LOG_NAME = "testlog";
	private static final int HTTP_SERVER_PORT = 45555;
	private static final int NUMBER_OF_TEST_ITEMS = 100;

	/* Define structure of a cube: names and types of dimensions and measures. */
	public static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("date", dateKey())
						.put("advertiser", intKey())
						.put("campaign", intKey())
						.put("banner", intKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("impressions", longSum())
						.put("clicks", longSum())
						.put("conversions", longSum())
						.put("revenue", doubleSum())
						.build());
	}

	/* Define aggregations and instantiate cube with the specified structure, eventloop and storages.
	Here we create the detailed aggregation over all 5 dimensions,
	which is also able to handle queries for any subset of its dimensions.
	We also define an aggregation for "date" dimension only
	to separately group records by date for fast queries that require only this dimension. */
	private static Cube getCube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                            CubeMetadataStorage cubeMetadataStorage, AggregationChunkStorage aggregationChunkStorage,
	                            AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, aggregationChunkStorage,
				cubeStructure, Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY, Aggregation.DEFAULT_SORTER_BLOCK_SIZE,
				Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE);
		cube.addAggregation("detailed", new AggregationMetadata(LogItem.DIMENSIONS, LogItem.MEASURES));
		cube.addAggregation("date", new AggregationMetadata(asList("date"), LogItem.MEASURES));
		cube.setChildParentRelationships(ImmutableMap.<String, String>builder()
				.put("campaign", "advertiser")
				.put("banner", "campaign")
				.build());
		return cube;
	}

	/* Load database access properties from file and instantiate Configuration for use by jOOQ. */
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

	private static Path getDirectory(String path) {
		Path directory = Paths.get(path);
		deleteRecursivelyQuietly(directory);
		return directory;
	}

	/* Instantiate LogToCubeMetadataStorage, which manages persistence of cube and logs metadata. */
	private static LogToCubeMetadataStorage getLogToCubeMetadataStorage(Eventloop eventloop,
	                                                                    ExecutorService executor,
	                                                                    Configuration jooqConfiguration,
	                                                                    CubeMetadataStorageSql aggregationMetadataStorage) {
		LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
				jooqConfiguration, aggregationMetadataStorage);
		metadataStorage.truncateTables();
		return metadataStorage;
	}

	/* Create AggregationChunkStorage, that controls saving and reading aggregated data chunks. */
	private static AggregationChunkStorage getAggregationChunkStorage(Eventloop eventloop, ExecutorService executor,
	                                                                  AggregationStructure structure,
	                                                                  Path aggregationsDir) {
		return new LocalFsChunkStorage(eventloop, executor, structure, aggregationsDir);
	}

	/* Instantiate LogManager, that serializes LogItem's and saves them as logs to LogFileSystem. */
	private static LogManager<LogItem> getLogManager(Eventloop eventloop, ExecutorService executor,
	                                                 DefiningClassLoader classLoader, Path logsDir) {
		LocalFsLogFileSystem fileSystem = new LocalFsLogFileSystem(eventloop, executor, logsDir);
		BufferSerializer<LogItem> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(LogItem.class);

		return new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);
	}

	/* Generate some test data and wrap it in StreamProducer. */
	private static StreamProducers.OfIterator<LogItem> getProducerOfRandomLogItems(Eventloop eventloop) {
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(NUMBER_OF_TEST_ITEMS);
		return new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems.iterator());
	}

	public static void main(String[] args) throws IOException {
		// used for database queries and some blocking IO operations
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		Path aggregationsDir = getDirectory(AGGREGATIONS_PATH);
		Path logsDir = getDirectory(LOGS_PATH);
		AggregationStructure structure = getStructure(classLoader);

		Configuration jooqConfiguration = getJooqConfiguration();
		AggregationChunkStorage aggregationChunkStorage =
				getAggregationChunkStorage(eventloop, executor, structure, aggregationsDir);
		CubeMetadataStorageSql aggregationMetadataStorage =
				new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration, "processId");
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, aggregationMetadataStorage);
		Cube cube = getCube(eventloop, executor, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage, structure);
		LogManager<LogItem> logManager = getLogManager(eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		/* Stream data to logs. */
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems = getProducerOfRandomLogItems(eventloop);
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		/* Read logs, aggregate data and save to cube. */
		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		/* Launch HTTP server, that accepts JSON queries to cube. */
		AsyncHttpServer server = CubeHttpServer.createServer(cube, eventloop, classLoader, HTTP_SERVER_PORT);
		server.listen();
		eventloop.run();

		executor.shutdown();
	}
}
