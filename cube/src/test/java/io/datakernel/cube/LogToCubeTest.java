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
import io.datakernel.aggregation_db.fieldtype.FieldTypeLong;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeInt;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallbackObserver;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
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
import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.cube.TestUtils.deleteRecursivelyQuietly;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogToCubeTest {
	private static final Logger logger = LoggerFactory.getLogger(LogToCubeTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static Cube newCube(Eventloop eventloop, DefiningClassLoader classLoader, CubeMetadataStorage cubeMetadataStorage,
	                           AggregationMetadataStorage aggregationMetadataStorage, AggregationChunkStorage aggregationChunkStorage,
	                           AggregationStructure aggregationStructure) {
		return new Cube(eventloop, classLoader, cubeMetadataStorage, aggregationMetadataStorage, aggregationChunkStorage,
				aggregationStructure, 1_000_000, 1_000_000, 30 * 60 * 1000, 10 * 60 * 1000);
	}

	public static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("pub", new KeyTypeInt())
						.put("adv", new KeyTypeInt())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("pubRequests", new FieldTypeLong())
						.put("advRequests", new FieldTypeLong())
						.build());
	}

	@Test
	public void testStubStorage() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		NioEventloop eventloop = new NioEventloop();
		AggregationMetadataStorageStub aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationChunkStorageStub aggregationStorage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure structure = getStructure(classLoader);
		LogToCubeMetadataStorageStub logToCubeMetadataStorageStub = new LogToCubeMetadataStorageStub(aggregationMetadataStorage);
		Cube cube = newCube(eventloop, classLoader, logToCubeMetadataStorageStub, aggregationMetadataStorage, aggregationStorage, structure);
		cube.addAggregation(new AggregationMetadata("pub", asList("pub"), asList("pubRequests")));
		cube.addAggregation(new AggregationMetadata("adv", asList("adv"), asList("advRequests")));

		ExecutorService executor = Executors.newCachedThreadPool();
		Path dir = temporaryFolder.newFolder().toPath();
		deleteRecursivelyQuietly(dir);
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, dir);
		BufferSerializer<TestPubRequest> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(TestPubRequest.class);

		LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

		LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
				"testlog", asList("partitionA"), logToCubeMetadataStorageStub);

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

		cube.loadChunks(ignoreCompletionCallback());

		eventloop.run();

		StreamConsumers.ToList<TestAdvResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(TestAdvResult.class, new AggregationQuery(asList("adv"), asList("advRequests")))
				.streamTo(consumerToList);
		eventloop.run();

		List<TestAdvResult> actualResults = consumerToList.getList();
		List<TestAdvResult> expectedResults = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		System.out.println(consumerToList.getList());

		assertEquals(expectedResults, actualResults);
	}

	public Runnable getLocalCubeRunnable(final Path logsDir, final Path aggregationsDir, final DefiningClassLoader classLoader,
	                                     final Configuration jooqConfiguration, final CountDownLatch latch,
	                                     final int partitionId) {
		return new Runnable() {
			@Override
			public void run() {
				ExecutorService executor = Executors.newCachedThreadPool();
				NioEventloop eventloop = new NioEventloop();
				AggregationMetadataStorageSql indexMetadataStorage = new AggregationMetadataStorageSql(eventloop, executor, jooqConfiguration);
				LogToCubeMetadataStorageSql cubeMetadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
						jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration), indexMetadataStorage);

				AggregationStructure structure = getStructure(classLoader);

				AggregationChunkStorage aggregationStorage = new LocalFsChunkStorage(eventloop, executor, structure, aggregationsDir);
				Cube cube = newCube(eventloop, classLoader, cubeMetadataStorage, indexMetadataStorage, aggregationStorage, structure);
				cube.addAggregation(new AggregationMetadata("pub", asList("pub"), asList("pubRequests")));
				cube.addAggregation(new AggregationMetadata("adv", asList("adv"), asList("advRequests")));

				CompletionCallbackObserver cb = new CompletionCallbackObserver();
				cube.saveAggregations(cb);

				eventloop.run();
				cb.check();

				LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, logsDir);
				BufferSerializer<TestPubRequest> bufferSerializer = SerializerBuilder
						.newDefaultInstance(classLoader)
						.create(TestPubRequest.class);

				LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

				LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
						"testlog", asList("partition" + partitionId), cubeMetadataStorage);

				int numberOfTestRequests = 100_000;
				List<TestPubRequest> pubRequests = TestUtils.generatePubRequests(numberOfTestRequests);

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
				AggregationStructure structure = getStructure(classLoader);
				AggregationMetadataStorageSql indexMetadataStorage = new AggregationMetadataStorageSql(eventloop, executor, jooqConfiguration);
				LogToCubeMetadataStorageSql cubeMetadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
						jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration), indexMetadataStorage);
				AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executor, structure,
						aggregationsDir);
				Cube cube = newCube(eventloop, classLoader, cubeMetadataStorage, indexMetadataStorage, aggregationChunkStorage, structure);

				CompletionCallbackObserver cb = new CompletionCallbackObserver();
				CompletionCallbackObserver cb2 = new CompletionCallbackObserver();
				cube.loadAggregations(cb);
				eventloop.run();

				cube.loadChunks(cb2);
				eventloop.run();

				cb.check();
				cb2.check();

				cube.consolidate(AsyncCallbacks.<Boolean>ignoreResultCallback());
				eventloop.run();

				latch.countDown();
			}
		};
	}

	@Test
	@Ignore
	public void testSqlStorage() throws Exception {
		Properties properties = new Properties();
		properties.load(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File("test.properties"))), UTF_8));
		HikariDataSource dataSource = new HikariDataSource(new HikariConfig(properties));

		Configuration jooqConfiguration = new DefaultConfiguration();
		jooqConfiguration.set(new DataSourceConnectionProvider(dataSource));
		jooqConfiguration.set(SQLDialect.MYSQL);

		DefiningClassLoader classLoader = new DefiningClassLoader();
		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		AggregationMetadataStorageSql indexMetadataStorage = new AggregationMetadataStorageSql(eventloop, executor, jooqConfiguration);
		LogToCubeMetadataStorageSql cubeMetadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
				jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration), indexMetadataStorage);
		cubeMetadataStorage.truncateTables();

		Path chunksDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		AggregationStructure structure = getStructure(classLoader);
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executor, structure,
				chunksDir);
		Cube cube = newCube(eventloop, classLoader, cubeMetadataStorage, indexMetadataStorage, aggregationChunkStorage, structure);
		cube.addAggregation(new AggregationMetadata("pub", asList("pub"), asList("pubRequests")));
		cube.addAggregation(new AggregationMetadata("adv", asList("adv"), asList("advRequests")));

		cube.saveAggregations(ignoreCompletionCallback());
		eventloop.run();

		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, logsDir);
		BufferSerializer<TestPubRequest> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(TestPubRequest.class);

		LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

		LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				TestAggregatorSplitter.factory(), "testlog", asList("partitionA"), cubeMetadataStorage);

		new StreamProducers.OfIterator<>(eventloop, asList(
				new TestPubRequest(1000, 1, asList(new TestPubRequest.TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestPubRequest.TestAdvRequest(10), new TestPubRequest.TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestPubRequest.TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.<TestPubRequest.TestAdvRequest>asList())).iterator())
				.streamTo(logManager.consumer("partitionA"));
		eventloop.run();

		logToCubeRunner.processLog(ignoreCompletionCallback());
		eventloop.run();

		cube.loadChunks(ignoreCompletionCallback());
		eventloop.run();

		/* ***** */

		new StreamProducers.OfIterator<>(eventloop, asList(
				new TestPubRequest(1000, 1, asList(new TestPubRequest.TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestPubRequest.TestAdvRequest(10), new TestPubRequest.TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestPubRequest.TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.<TestPubRequest.TestAdvRequest>asList())).iterator())
				.streamTo(logManager.consumer("partitionA"));
		eventloop.run();

		logToCubeRunner.processLog(ignoreCompletionCallback());
		eventloop.run();

		cube.loadChunks(ignoreCompletionCallback());
		eventloop.run();

		AggregationQuery query = new AggregationQuery(asList("adv"), asList("advRequests"));
		StreamConsumers.ToList<TestAdvResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(TestAdvResult.class, query).streamTo(consumerToList);
		eventloop.run();

		cube.consolidate(AsyncCallbacks.<Boolean>ignoreResultCallback());
		eventloop.run();

		cube.loadChunks(ignoreCompletionCallback());
		eventloop.run();

		cube.consolidate(AsyncCallbacks.<Boolean>ignoreResultCallback());
		eventloop.run();

		cube.loadChunks(ignoreCompletionCallback());
		eventloop.run();

		StreamConsumers.ToList<TestAdvResult> consumerToList2 = StreamConsumers.toList(eventloop);
		cube.query(TestAdvResult.class, query).streamTo(consumerToList2);
		eventloop.run();

		assertEquals(consumerToList.getList(), consumerToList2.getList());
	}

	@Test
	@Ignore
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
		AggregationMetadataStorageSql indexMetadataStorage = new AggregationMetadataStorageSql(eventloop, executor, jooqConfiguration);
		LogToCubeMetadataStorageSql cubeMetadataStorage = new LogToCubeMetadataStorageSql(eventloop, executor,
				jooqConfiguration, new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration),
				indexMetadataStorage);
		cubeMetadataStorage.truncateTables();

		final Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		for (int i = 0; i < threads; ++i) {
			Runnable localCubeRunnable = getLocalCubeRunnable(logsDir, aggregationsDir, classLoader, jooqConfiguration, latch, i);
			Thread thread = new Thread(localCubeRunnable);
			thread.start();
		}

		latch.await();

		CompletionCallbackObserver cb = new CompletionCallbackObserver();

		final AggregationStructure structure = getStructure(classLoader);
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executor, structure,
				aggregationsDir);
		Cube cube = newCube(eventloop, classLoader, cubeMetadataStorage, indexMetadataStorage, aggregationChunkStorage, structure);
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
//		cube.reloadAllChunksConsolidations(cb); TODO (dtkachenko)
		eventloop.run();

		cb.check();
		cb = new CompletionCallbackObserver();
//		cube.reloadAllChunksConsolidations(cb); TODO (dtkachenko)
		eventloop.run();

		cb.check();
		cb = new CompletionCallbackObserver();
//		cube.reloadAllChunksConsolidations(cb); TODO (dtkachenko)
		eventloop.run();
	}

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
}
