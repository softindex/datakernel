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
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.asm.DefiningClassLoader;
import io.datakernel.async.CompletionCallbackObserver;
import io.datakernel.cube.api.CubePredicatesGsonSerializer;
import io.datakernel.cube.api.CubeQueryGsonSerializer;
import io.datakernel.cube.dimensiontype.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopStub;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.logfs.LogFileSystemImpl;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogManagerImpl;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerFactory;
import io.datakernel.serializer.SerializerScanner;
import io.datakernel.serializer.asm.SerializerGen;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Charsets.UTF_8;
import static io.datakernel.cube.MeasureType.SUM_LONG;
import static io.datakernel.cube.Utils.deleteRecursivelyQuietly;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogToCubeTest {
	private static ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
	private static final Logger logger = LoggerFactory.getLogger(LogToCubeTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static final class TestPubResult {
		public int pub;
		public long pubRequests;

		@Override
		public String toString() {
			return "TestResult{pub=" + pub + ", pubRequests=" + pubRequests + '}';
		}
	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

		public TestAdvResult() {
		}

		public TestAdvResult(int adv, long advRequests) {
			this.adv = adv;
			this.advRequests = advRequests;
		}

		@Override
		public String toString() {
			return "TestAdvResult{adv=" + adv + ", advRequests=" + advRequests + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestAdvResult that = (TestAdvResult) o;

			if (adv != that.adv) return false;
			if (advRequests != that.advRequests) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = adv;
			result = 31 * result + (int) (advRequests ^ (advRequests >>> 32));
			return result;
		}
	}

	public static Cube newCube(Eventloop eventloop, DefiningClassLoader classLoader, CubeMetadataStorage metadataStorage, AggregationStorage aggregationStorage,
	                           CubeStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, metadataStorage, aggregationStorage,
				cubeStructure, 1_000_000, 1_000_000, 30 * 60 * 1000, 10 * 60 * 1000);
		return cube;
	}

	public static CubeStructure getStructure(DefiningClassLoader classLoader) {
		return new CubeStructure(classLoader,
				ImmutableMap.<String, DimensionType>builder()
						.put("pub", new DimensionTypeInt())
						.put("adv", new DimensionTypeInt())
						.build(),
				ImmutableMap.<String, MeasureType>builder()
						.put("pubRequests", SUM_LONG)
						.put("advRequests", SUM_LONG)
						.build());
	}

	@Test
	public void testStubStorage() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		EventloopStub eventloop = new EventloopStub();
		AggregationStorageStub aggregationStorage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure structure = getStructure(classLoader);
		Cube cube = newCube(eventloop, classLoader, new LogToCubeMetadataStorageStub(), aggregationStorage, structure);
		cube.addAggregation(new Aggregation("pub", asList("pub"), asList("pubRequests")));
		cube.addAggregation(new Aggregation("adv", asList("adv"), asList("advRequests")));

		ExecutorService executor = Executors.newCachedThreadPool();
		Path dir = temporaryFolder.newFolder().toPath();
		deleteRecursivelyQuietly(dir);
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, dir);
		SerializerFactory bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory(classLoader, true, true);
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerGen serializerGen = registry.serializer(TypeToken.of(TestPubRequest.class));
		BufferSerializer<TestPubRequest> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

		LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

		LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
				"testlog", asList("partitionA"), new LogToCubeMetadataStorageStub());

		new StreamProducers.OfIterator<>(eventloop, asList(
				new TestPubRequest(1000, 1, asList(new TestPubRequest.TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestPubRequest.TestAdvRequest(10), new TestPubRequest.TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestPubRequest.TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.<TestPubRequest.TestAdvRequest>asList())).iterator())
				.streamTo(logManager.consumer("partitionA"));

		eventloop.run();

		CompletionCallbackObserver cb = new CompletionCallbackObserver();
		logToCubeRunner.processLog(cb);

		eventloop.run();
		cb.check();

		StreamConsumers.ToList<TestAdvResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, TestAdvResult.class, new CubeQuery(asList("adv"), asList("advRequests")))
				.streamTo(consumerToList);
		eventloop.run();

		List<TestAdvResult> actualResults = consumerToList.getList();
		List<TestAdvResult> expectedResults = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		System.out.println(consumerToList.getList());

		assertEquals(expectedResults, actualResults);
	}

	@Ignore
	@Test
	public void testSqlStorage() throws Exception {
		Properties properties = new Properties();
		properties.load(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File("test.properties"))), UTF_8));
		HikariDataSource dataSource = new HikariDataSource(new HikariConfig(properties));

		Configuration jooqConfiguration = new DefaultConfiguration();
		jooqConfiguration.set(new DataSourceConnectionProvider(dataSource));
		jooqConfiguration.set(SQLDialect.MYSQL);

		DefiningClassLoader classLoader = new DefiningClassLoader();
		ExecutorService executor = Executors.newCachedThreadPool();
		EventloopStub eventloop = new EventloopStub();
		LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor, jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration));

		CubeStructure structure = getStructure(classLoader);

		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		deleteRecursivelyQuietly(aggregationsDir);
		AggregationStorage aggregationStorage = new LocalFsAggregationStorage(eventloop, executor, structure, aggregationsDir);
		metadataStorage.truncateTables();
		Cube cube = newCube(eventloop, classLoader, metadataStorage, aggregationStorage, structure);
		cube.addAggregation(new Aggregation("pub", asList("pub"), asList("pubRequests")));
		cube.addAggregation(new Aggregation("adv", asList("adv"), asList("advRequests")));

		CompletionCallbackObserver cb = new CompletionCallbackObserver();
		cube.saveAggregations(cb);

		eventloop.run();
		cb.check();

		CubeQuery query = new CubeQuery(asList("adv"), asList("advRequests"));

		Path logsDir = temporaryFolder.newFolder().toPath();
		deleteRecursivelyQuietly(logsDir);
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, logsDir);
		SerializerFactory bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory(classLoader, true, true);
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerGen serializerGen = registry.serializer(TypeToken.of(TestPubRequest.class));
		BufferSerializer<TestPubRequest> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

		LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

		LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
				"testlog", asList("partitionA"), metadataStorage);

		new StreamProducers.OfIterator<>(eventloop, asList(
				new TestPubRequest(1000, 1, asList(new TestPubRequest.TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestPubRequest.TestAdvRequest(10), new TestPubRequest.TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestPubRequest.TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.<TestPubRequest.TestAdvRequest>asList())).iterator())
				.streamTo(logManager.consumer("partitionA"));
		eventloop.run();

		cb = new CompletionCallbackObserver();
		logToCubeRunner.processLog(cb);
		eventloop.run();
		cb.check();

		StreamConsumers.ToList<TestAdvResult> consumerToList1 = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, TestAdvResult.class, query).streamTo(consumerToList1);
		eventloop.run();

		System.out.println(consumerToList1.getList());
		List<TestAdvResult> expectedResult = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));
		List<TestAdvResult> actualResult = consumerToList1.getList();
		assertEquals(expectedResult, actualResult);

		new StreamProducers.OfIterator<>(eventloop, asList(
				new TestPubRequest(1000, 1, asList(new TestPubRequest.TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestPubRequest.TestAdvRequest(10), new TestPubRequest.TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestPubRequest.TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.<TestPubRequest.TestAdvRequest>asList())).iterator())
				.streamTo(logManager.consumer("partitionA"));
		eventloop.run();

		cb = new CompletionCallbackObserver();
		logToCubeRunner.processLog(cb);
		eventloop.run();
		cb.check();

		StreamConsumers.ToList<TestAdvResult> consumerToList;
		consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, TestAdvResult.class, query).streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());
		List<TestAdvResult> expectedResult2 = asList(new TestAdvResult(10, 4), new TestAdvResult(20, 2), new TestAdvResult(30, 2));
		List<TestAdvResult> actualResult2 = consumerToList.getList();
		assertEquals(expectedResult2, actualResult2);

		cb = new CompletionCallbackObserver();
		cube.consolidate(cb);
		eventloop.run();
		cb.check();

		cb = new CompletionCallbackObserver();
		cube.consolidate(cb);
		eventloop.run();
		cb.check();

		consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, TestAdvResult.class, query).streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());
		assertEquals(expectedResult2, consumerToList.getList());

		cb = new CompletionCallbackObserver();
		Cube cube2 = newCube(eventloop, classLoader, metadataStorage, aggregationStorage, structure);
		cube2.loadAggregations(cb);
		eventloop.run();
		cb.check();

		cb = new CompletionCallbackObserver();
		cube2.loadChunks(1, cb);
		eventloop.run();
		cb.check();

		consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube2.query(0, TestAdvResult.class, query).streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());
		assertEquals(expectedResult, consumerToList.getList());

		cb = new CompletionCallbackObserver();
		cube2.loadChunks(4, cb);
		eventloop.run();
		cb.check();

		consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube2.query(1, TestAdvResult.class, query).streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());
		assertEquals(expectedResult, consumerToList.getList());

		cb = new CompletionCallbackObserver();
		Cube cube3 = newCube(eventloop, classLoader, metadataStorage, aggregationStorage, structure);
		cube3.loadAggregations(cb);
		eventloop.run();
		cb.check();

		cb = new CompletionCallbackObserver();
		cube3.loadChunks(4, cb);
		eventloop.run();
		cb.check();

		consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube3.query(0, TestAdvResult.class, query).streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());
		assertEquals(expectedResult2, consumerToList.getList());

		executor.shutdownNow();
	}

	public Runnable getLocalCubeRunnable(final Path logsDir, final Path aggregationsDir, final DefiningClassLoader classLoader,
	                                     final Configuration jooqConfiguration, final CountDownLatch latch,
	                                     final int partitionId) {
		return new Runnable() {
			@Override
			public void run() {
				ExecutorService executor = Executors.newCachedThreadPool();
				EventloopStub eventloop = new EventloopStub();
				LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor, jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration));

				CubeStructure structure = getStructure(classLoader);

				AggregationStorage aggregationStorage = new LocalFsAggregationStorage(eventloop, executor, structure, aggregationsDir);
				Cube cube = newCube(eventloop, classLoader, metadataStorage, aggregationStorage, structure);
				cube.addAggregation(new Aggregation("pub", asList("pub"), asList("pubRequests")));
				cube.addAggregation(new Aggregation("adv", asList("adv"), asList("advRequests")));

				CompletionCallbackObserver cb = new CompletionCallbackObserver();
				cube.saveAggregations(cb);

				eventloop.run();
				cb.check();

				LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, logsDir);
				SerializerFactory bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory(classLoader, true, true);
				SerializerScanner registry = SerializerScanner.defaultScanner();
				SerializerGen serializerGen = registry.serializer(TypeToken.of(TestPubRequest.class));
				BufferSerializer<TestPubRequest> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

				LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

				LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
						"testlog", asList("partition" + partitionId), metadataStorage);

				int numberOfTestRequests = 100_000;
				List<TestPubRequest> pubRequests = CubeBenchmark.generatePubRequests(numberOfTestRequests);

				new StreamProducers.OfIterator<>(eventloop, pubRequests.iterator())
						.streamTo(logManager.consumer("partition" + partitionId));
				eventloop.run();

				cb = new CompletionCallbackObserver();
				logToCubeRunner.processLog(cb);
				eventloop.run();

				latch.countDown();
			}
		};
	}

	public Runnable getConsolidationRunnable(final CountDownLatch latch, final DefiningClassLoader classLoader,
	                                         final Configuration jooqConfiguration, final Path aggregationsDir) {
		return new Runnable() {
			@Override
			public void run() {
				ExecutorService executor = Executors.newCachedThreadPool();
				NioEventloop eventloop = new NioEventloop();
				CubeStructure structure = getStructure(classLoader);
				LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
						jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration));
				AggregationStorage aggregationStorage = new LocalFsAggregationStorage(eventloop, executor, structure,
						aggregationsDir);
				Cube cube = newCube(eventloop, classLoader, metadataStorage, aggregationStorage, structure);

				CompletionCallbackObserver cb = new CompletionCallbackObserver();
				CompletionCallbackObserver cb2 = new CompletionCallbackObserver();
				cube.loadAggregations(cb);
				cube.loadChunks(cb2);
				eventloop.run();

				cb.check();
				cb2.check();

				cb = new CompletionCallbackObserver();
				cube.consolidate(cb);
				eventloop.run();
				cb.check();

				latch.countDown();
			}
		};
	}

	@Ignore
	@Test
	public void testConcurrently() throws Exception {
		final DefiningClassLoader classLoader = new DefiningClassLoader();
		Properties properties = new Properties();
		properties.load(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File("test.properties"))), UTF_8));
		HikariDataSource dataSource = new HikariDataSource(new HikariConfig(properties));

		final Configuration jooqConfiguration = new DefaultConfiguration();
		jooqConfiguration.set(new DataSourceConnectionProvider(dataSource));
		jooqConfiguration.set(SQLDialect.MYSQL);

		int threads = 10;
		CountDownLatch latch = new CountDownLatch(threads);

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		LogToCubeMetadataStorageSql metadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor, jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration));
		metadataStorage.truncateTables();

		final Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		for (int i = 0; i < threads; ++i) {
			Runnable localCubeRunnable = getLocalCubeRunnable(logsDir, aggregationsDir, classLoader, jooqConfiguration, latch, i);
			Thread thread = new Thread(localCubeRunnable);
			thread.start();
		}

		latch.await();

		CompletionCallbackObserver cb = new CompletionCallbackObserver();

		final CubeStructure structure = getStructure(classLoader);
		AggregationStorage aggregationStorage = new LocalFsAggregationStorage(eventloop, executor, structure, aggregationsDir);
		Cube cube = newCube(eventloop, classLoader, metadataStorage, aggregationStorage, structure);
		cube.loadAggregations(cb);
		cube.loadChunks(cb);
		eventloop.run();

		int consolidationThreads = 4;
		CountDownLatch consolidationLatch = new CountDownLatch(consolidationThreads);

		logger.info("Beginning concurrent consolidation.");
		for (int i = 0; i < consolidationThreads; ++i) {
			Runnable consolidationRunnable = getConsolidationRunnable(consolidationLatch, classLoader, jooqConfiguration, aggregationsDir);
			new Thread(consolidationRunnable).start();
		}

		consolidationLatch.await();

		cb.check();
		cb = new CompletionCallbackObserver();
		cube.reloadAllChunksConsolidations(cb);
		eventloop.run();

		cb.check();
		cb = new CompletionCallbackObserver();
		cube.reloadAllChunksConsolidations(cb);
		eventloop.run();

		cb.check();
		cube.removeOldChunks();
		eventloop.run();
	}

	public static CubeStructure cubeStructureForGsonTest(DefiningClassLoader classLoader) {
		return new CubeStructure(classLoader,
				ImmutableMap.<String, DimensionType>builder()
						.put("key1", new DimensionTypeInt())
						.put("key2", new DimensionTypeString())
						.put("key3", new DimensionTypeDouble())
						.put("key4", new DimensionTypeDate())
						.put("key5", new DimensionTypeShort())
						.put("key6", new DimensionTypeFloat())
						.build(),
				ImmutableMap.<String, MeasureType>builder()
						.put("metric1", SUM_LONG)
						.put("metric2", SUM_LONG)
						.put("metric3", SUM_LONG)
						.build());
	}

	@Test
	public void testGson() {
		CubeQuery query = new CubeQuery()
				.dimensions("key1", "key2", "key3", "key4", "key5", "key6")
				.measures("metric1", "metric2", "metric3")
				.eq("key1", 11)
				.eq("key2", "string")
				.eq("key3", 666.17)
				.between("key4", 12000, 15000)
				.eq("key5", (short) 12)
				.eq("key6", 1000.111111843257f)
				.orderAsc("metric1")
				.orderDesc("metric3");

		Gson gson = new GsonBuilder()
				.registerTypeAdapter(CubeQuery.class, new CubeQueryGsonSerializer())
				.registerTypeAdapter(CubeQuery.CubePredicates.class, new CubePredicatesGsonSerializer(cubeStructureForGsonTest(new DefiningClassLoader())))
				.create();

		String s = gson.toJson(query);
		System.out.println(s);

		CubeQuery query1 = gson.fromJson(s, CubeQuery.class);
		System.out.println(query1);

		assertEquals(query, query1);
	}

}