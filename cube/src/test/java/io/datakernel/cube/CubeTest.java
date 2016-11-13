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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.aggregation.*;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.async.*;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.simplefs.SimpleFsServer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.AggregationPredicates.*;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.async.AsyncRunnables.runInParallel;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeTest {
	private static Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CubeTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	static {
		root.setLevel(Level.TRACE);
	}

	public static Cube newCube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                           AggregationChunkStorage storage) {
		CubeMetadataStorageStub cubeMetadataStorage = new CubeMetadataStorageStub();
		return Cube.create(eventloop, executorService, classLoader, cubeMetadataStorage, storage)
				.withDimension("key1", FieldTypes.ofInt())
				.withDimension("key2", FieldTypes.ofInt())
				.withMeasure("metric1", sum(ofLong()))
				.withMeasure("metric2", sum(ofLong()))
				.withMeasure("metric3", sum(ofLong()))
				.withAggregation(id("detailedAggregation").withDimensions("key1", "key2").withMeasures("metric1", "metric2", "metric3"));
	}

	public static Cube newSophisticatedCube(Eventloop eventloop, ExecutorService executorService,
	                                        DefiningClassLoader classLoader, AggregationChunkStorage storage) {
		CubeMetadataStorageStub cubeMetadataStorage = new CubeMetadataStorageStub();
		return Cube.create(eventloop, executorService, classLoader, cubeMetadataStorage, storage)
				.withDimension("key1", FieldTypes.ofInt())
				.withDimension("key2", FieldTypes.ofInt())
				.withDimension("key3", FieldTypes.ofInt())
				.withDimension("key4", FieldTypes.ofInt())
				.withDimension("key5", FieldTypes.ofInt())
				.withMeasure("metric1", sum(ofLong()))
				.withMeasure("metric2", sum(ofLong()))
				.withMeasure("metric3", sum(ofLong()))
				.withAggregation(id("detailedAggregation").withDimensions("key1", "key2", "key3", "key4", "key5").withMeasures("metric1", "metric2", "metric3"));
	}

	@Test
	public void testQuery1() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(new DataItemResult(1, 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	private static final int LISTEN_PORT = 45578;

	private SimpleFsServer prepareServer(Eventloop eventloop, Path serverStorage) throws IOException {
		final ExecutorService executor = Executors.newCachedThreadPool();
		SimpleFsServer fileServer = SimpleFsServer.create(eventloop, executor, serverStorage)
				.withListenPort(LISTEN_PORT);
		fileServer.listen();
		return fileServer;
	}

	private void stop(SimpleFsServer server) {
		server.close(IgnoreCompletionCallback.create());
	}

	@Test
	public void testSimpleFsAggregationStorage() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		Path serverStorage = temporaryFolder.newFolder("storage").toPath();
		final SimpleFsServer simpleFsServer1 = prepareServer(eventloop, serverStorage);

		AggregationChunkStorage storage = SimpleFsChunkStorage.create(eventloop,
				new InetSocketAddress(InetAddress.getLocalHost(), LISTEN_PORT));
		final Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);

		runInParallel(eventloop,
				new AsyncRunnable() {
					@Override
					public void run(CompletionCallback callback) {
						final StreamConsumer<DataItem1> cubeConsumer1 = cube.consumer(DataItem1.class, DataItem1.DIMENSIONS,
								DataItem1.METRICS, new CommitCallbackStub(cube, callback));
						StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
								.streamTo(cubeConsumer1);
					}
				},
				new AsyncRunnable() {
					@Override
					public void run(CompletionCallback callback) {
						final StreamConsumer<DataItem2> cubeConsumer2 = cube.consumer(DataItem2.class, DataItem2.DIMENSIONS,
								DataItem2.METRICS, new CommitCallbackStub(cube, callback));
						StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
								.streamTo(cubeConsumer2);
					}
				}
		).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				logger.info("Streaming to SimpleFS succeeded.");
				stop(simpleFsServer1);
			}
		});

		eventloop.run();

		final SimpleFsServer simpleFsServer2 = prepareServer(eventloop, serverStorage);
		final StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		consumerToList.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Streaming query {} result from SimpleFS succeeded.");
				stop(simpleFsServer2);
			}
		});
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(new DataItemResult(1, 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testOrdering() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 30, 25), new DataItem1(1, 3, 40, 10),
				new DataItem1(1, 4, 23, 48), new DataItem1(1, 3, 4, 18)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 15, 5), new DataItem2(1, 4, 55, 20),
				new DataItem2(1, 2, 12, 42), new DataItem2(1, 4, 58, 22)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"), alwaysTrue(),
				DataItemResult.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(
				new DataItemResult(1, 2, 30, 37, 42),   // metric2 =  37
				new DataItemResult(1, 3, 44, 43, 5),    // metric2 =  43
				new DataItemResult(1, 4, 23, 161, 42)); // metric2 = 161

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testMultipleOrdering() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 3, 30, 25), new DataItem1(1, 4, 40, 10),
				new DataItem1(1, 5, 23, 48), new DataItem1(1, 6, 4, 18)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 7, 15, 5), new DataItem2(1, 8, 55, 20),
				new DataItem2(1, 9, 12, 42), new DataItem2(1, 10, 58, 22)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"), alwaysTrue(),
				DataItemResult.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(
				new DataItemResult(1, 3, 30, 25, 0),   // metric1 = 30, metric2 = 25
				new DataItemResult(1, 4, 40, 10, 0),   // metric1 = 40, metric2 = 10
				new DataItemResult(1, 5, 23, 48, 0),   // metric1 = 23, metric2 = 48
				new DataItemResult(1, 6, 4, 18, 0),   // metric1 =  4, metric2 = 18
				new DataItemResult(1, 7, 0, 15, 5),   // metric1 =  0, metric2 = 15
				new DataItemResult(1, 8, 0, 55, 20),   // metric1 =  0, metric2 = 55
				new DataItemResult(1, 9, 0, 12, 42),   // metric1 =  0, metric2 = 12
				new DataItemResult(1, 10, 0, 58, 22));  // metric1 =  0, metric2 = 58

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testBetweenPredicate() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(14, 1, 30, 25),
				new DataItem1(13, 3, 40, 10),
				new DataItem1(9, 4, 23, 48),
				new DataItem1(6, 3, 4, 18),
				new DataItem1(10, 5, 22, 16),
				new DataItem1(20, 7, 13, 49),
				new DataItem1(15, 9, 11, 12),
				new DataItem1(5, 99, 40, 36)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem2(9, 3, 15, 5),
				new DataItem2(11, 4, 55, 20),
				new DataItem2(17, 2, 12, 42),
				new DataItem2(11, 4, 58, 22),
				new DataItem2(19, 18, 22, 55),
				new DataItem2(7, 14, 28, 6),
				new DataItem2(8, 42, 33, 17),
				new DataItem2(5, 77, 88, 98)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(between("key1", 5, 10), between("key2", 40, 1000)),
				DataItemResult.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(
				new DataItemResult(5, 77, 0, 88, 98),
				new DataItemResult(5, 99, 40, 36, 0),
				new DataItemResult(8, 42, 0, 33, 17));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testBetweenTransformation() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		Cube cube = newSophisticatedCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem3(14, 1, 42, 25, 53, 30, 25),
				new DataItem3(13, 3, 49, 13, 50, 40, 10),
				new DataItem3(9, 4, 59, 17, 79, 23, 48),
				new DataItem3(6, 3, 30, 20, 63, 4, 18),
				new DataItem3(10, 5, 33, 21, 69, 22, 16),
				new DataItem3(20, 7, 39, 29, 65, 13, 49),
				new DataItem3(15, 9, 57, 26, 59, 11, 12),
				new DataItem3(5, 99, 35, 27, 76, 40, 36)))
				.streamTo(cube.consumer(DataItem3.class, DataItem3.DIMENSIONS, DataItem3.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem4(9, 3, 41, 11, 65, 15, 5),
				new DataItem4(11, 4, 38, 10, 68, 55, 20),
				new DataItem4(17, 2, 40, 15, 52, 12, 42),
				new DataItem4(11, 4, 47, 22, 60, 58, 22),
				new DataItem4(19, 18, 52, 24, 80, 22, 55),
				new DataItem4(7, 14, 31, 14, 73, 28, 6),
				new DataItem4(8, 42, 46, 19, 75, 33, 17),
				new DataItem4(5, 77, 50, 20, 56, 88, 98)))
				.streamTo(cube.consumer(DataItem4.class, DataItem4.DIMENSIONS, DataItem4.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult3> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2", "key3", "key4", "key5"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", 5), between("key2", 75, 99), between("key3", 35, 50), eq("key4", 20), eq("key5", 56)),
				DataItemResult3.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult3> actual = consumerToList.getList();
		List<DataItemResult3> expected = Collections.singletonList(new DataItemResult3(5, 77, 50, 20, 56, 0, 88, 98));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testGrouping() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20),
				new DataItem1(1, 2, 15, 25), new DataItem1(1, 1, 95, 85), new DataItem1(2, 1, 55, 65),
				new DataItem1(1, 4, 5, 35)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 20, 10), new DataItem2(1, 4, 10, 20),
				new DataItem2(1, 1, 80, 75)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult2> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key2"), asList("metric1", "metric2", "metric3"),
				alwaysTrue(),
				DataItemResult2.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		// SELECT key1, SUM(metric1), SUM(metric2), SUM(metric3) FROM detailedAggregation WHERE key1 = 1 AND key2 = 3 GROUP BY key1
		eventloop.run();

		List<DataItemResult2> actual = consumerToList.getList();
		List<DataItemResult2> expected = asList(new DataItemResult2(1, 150, 230, 75), new DataItemResult2(2, 25, 45, 0),
				new DataItemResult2(3, 10, 40, 10), new DataItemResult2(4, 5, 45, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testQuery2() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executorService = newSingleThreadExecutor();
		Path dir = temporaryFolder.newFolder().toPath();
		AggregationChunkStorage storage = LocalFsChunkStorage.create(eventloop, executorService, dir);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(new DataItemResult(1, 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testConsolidate() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CommitCallbackStub(cube)));
		eventloop.run();

		cube.setLastReloadTimestamp(eventloop.currentTimeMillis());
		ResultCallbackFuture<Boolean> future = ResultCallbackFuture.create();
		cube.consolidate(future);

		eventloop.run();
		assertEquals(true, future.get());

		future = ResultCallbackFuture.create();
		cube.consolidate(future);

		eventloop.run();
		assertEquals(true, future.get());

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 4)),
				DataItemResult.class, DefiningClassLoader.create(classLoader)
		).streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(new DataItemResult(1, 4, 0, 30, 60));

		assertEquals(expected, actual);
	}

}