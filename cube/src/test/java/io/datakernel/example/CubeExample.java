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

package io.datakernel.example;

import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.async.CompletionCallbackObserver;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.*;
import io.datakernel.cube.dimensiontype.DimensionType;
import io.datakernel.cube.dimensiontype.DimensionTypeInt;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.logfs.LogFileSystemImpl;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogManagerImpl;
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
import static io.datakernel.cube.MeasureType.SUM_DOUBLE;
import static io.datakernel.cube.MeasureType.SUM_LONG;
import static io.datakernel.cube.Utils.deleteRecursivelyQuietly;
import static io.datakernel.cube.api.HttpJsonApiServer.httpServer;
import static java.util.Arrays.asList;

/*
Example, that describes the essential steps to get the cube up and running, along with log processing tools
and HTTP server, that allows users to perform JSON queries.
To run this example, do the following:
1. Create *.properties file with database access configuration (driver, URL, username, password)
and specify the path to this file in DATABASE_PROPERTIES_PATH constant
(it is advised to launch examples in $MODULE_DIR$, so paths are resolved properly).
2. Specify SQLDialect in DATABASE_DIALECT constant according to RDBMS you are using.
3. (Optional) Change directories (AGGREGATIONS_PATH and LOGS_PATH) used to store test data.
4. (Optional) Change names of log partitions (LOG_PARTITION_NAME and LOG_PARTITIONS).
5. (Optional) Define another port to be used by HTTP server (HTTP_SERVER_PORT).
6. (Optional) Adjust the size of test data set (NUMBER_OF_TEST_ITEMS).
7. Run main() and enjoy.
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
	public static CubeStructure getStructure(DefiningClassLoader classLoader) {
		return new CubeStructure(classLoader,
				ImmutableMap.<String, DimensionType>builder()
						.put("date", new DimensionTypeInt())
						.put("publisher", new DimensionTypeInt())
						.put("advertiser", new DimensionTypeInt())
						.put("campaign", new DimensionTypeInt())
						.put("banner", new DimensionTypeInt())
						.build(),
				ImmutableMap.<String, MeasureType>builder()
						.put("impressions", SUM_LONG)
						.put("clicks", SUM_LONG)
						.put("conversions", SUM_LONG)
						.put("revenue", SUM_DOUBLE)
						.build());
	}

	/* Define aggregations and instantiate cube with the specified structure, eventloop and storages.
	Here we only create one aggregation that contains all the dimensions and measures of LogItem. */
	private static Cube getCube(NioEventloop eventloop, DefiningClassLoader classLoader,
	                            CubeMetadataStorage metadataStorage, AggregationStorage aggregationStorage,
	                            CubeStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, metadataStorage, aggregationStorage, cubeStructure);
		cube.addAggregation(new Aggregation("detailed", LogItem.DIMENSIONS, LogItem.MEASURES));
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
	private static LogToCubeMetadataStorage getLogToCubeMetadataStorage(NioEventloop eventloop,
	                                                                    ExecutorService executor,
	                                                                    Configuration jooqConfiguration) {
		CubeMetadataStorageSql cubeMetadataStorage =
				new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration);
		LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
				jooqConfiguration, cubeMetadataStorage);
		metadataStorage.truncateTables();
		return metadataStorage;
	}

	/* Create AggregationStorage, that controls saving and reading aggregated data chunks. */
	private static AggregationStorage getAggregationStorage(NioEventloop eventloop, ExecutorService executor,
	                                                        CubeStructure structure, Path aggregationsDir) {
		return new LocalFsAggregationStorage(eventloop, executor, structure, aggregationsDir);
	}

	/* Instantiate LogManager, that serializes LogItem's and saves them as logs to LogFileSystem. */
	private static LogManager<LogItem> getLogManager(NioEventloop eventloop, ExecutorService executor,
	                                                 DefiningClassLoader classLoader, Path logsDir) {
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, logsDir);
		BufferSerializer<LogItem> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(LogItem.class);

		LogManager<LogItem> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

		return logManager;
	}

	/* Generate some test data and wrap it in StreamProducer. */
	private static StreamProducers.OfIterator<LogItem> getProducerOfRandomLogItems(NioEventloop eventloop) {
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(NUMBER_OF_TEST_ITEMS);
		return new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems.iterator());
	}

	public static void main(String[] args) throws IOException {
		// used for database queries and some blocking IO operations
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = new DefiningClassLoader();
		NioEventloop eventloop = new NioEventloop();
		Path aggregationsDir = getDirectory(AGGREGATIONS_PATH);
		Path logsDir = getDirectory(LOGS_PATH);
		CubeStructure structure = getStructure(classLoader);

		Configuration jooqConfiguration = getJooqConfiguration();
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration);
		AggregationStorage aggregationStorage =
				getAggregationStorage(eventloop, executor, structure, aggregationsDir);
		Cube cube = getCube(eventloop, classLoader, logToCubeMetadataStorage, aggregationStorage, structure);
		LogManager<LogItem> logManager = getLogManager(eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		/* Save the specified aggregations to metadata storage. */
		CompletionCallbackObserver cb = new CompletionCallbackObserver();
		cube.saveAggregations(cb);
		eventloop.run();
		cb.check();

		/* Stream data to logs. */
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems = getProducerOfRandomLogItems(eventloop);
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		/* Read logs, aggregate data and save to cube. */
		logToCubeRunner.processLog(cb);
		eventloop.run();

		/* Launch HTTP server, that accepts JSON queries to cube. */
		AsyncHttpServer server = httpServer(cube, eventloop, HTTP_SERVER_PORT);
		server.listen();
		eventloop.run();

		executor.shutdown();
	}
}
