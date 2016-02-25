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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.bean.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.simplefs.SimpleFsServer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation_db.AggregationChunk.createChunk;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.longSum;
import static io.datakernel.aggregation_db.keytype.KeyTypes.intKey;
import static io.datakernel.async.AsyncCallbacks.waitAll;
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
	                           AggregationChunkStorage storage, AggregationStructure aggregationStructure) {
		CubeMetadataStorageStub cubeMetadataStorage = new CubeMetadataStorageStub();
		Cube cube = new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, storage,
				aggregationStructure, Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY, Aggregation.DEFAULT_SORTER_BLOCK_SIZE,
				Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE);
		cube.addAggregation("detailedAggregation", new AggregationMetadata(asList("key1", "key2"),
				asList("metric1", "metric2", "metric3")));
		return cube;
	}

	public static Cube newSophisticatedCube(Eventloop eventloop, ExecutorService executorService,
	                                        DefiningClassLoader classLoader, AggregationChunkStorage storage,
	                                        AggregationStructure aggregationStructure) {
		CubeMetadataStorageStub cubeMetadataStorage = new CubeMetadataStorageStub();
		Cube cube = new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, storage, aggregationStructure,
				Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY, Aggregation.DEFAULT_SORTER_BLOCK_SIZE,
				Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE);
		cube.addAggregation("detailedAggregation", new AggregationMetadata(asList("key1", "key2", "key3", "key4", "key5"),
				asList("metric1", "metric2", "metric3")));
		return cube;
	}

	public static AggregationStructure cubeStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("key1", intKey())
						.put("key2", intKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("metric1", longSum())
						.put("metric2", longSum())
						.put("metric3", longSum())
						.build());
	}

	public static AggregationStructure sophisticatedCubeStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("key1", intKey())
						.put("key2", intKey())
						.put("key3", intKey())
						.put("key4", intKey())
						.put("key5", intKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("metric1", longSum())
						.put("metric2", longSum())
						.put("metric3", longSum())
						.build());
	}

	@Test
	public void testQuery1() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure aggregationStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult.class,
				new CubeQuery()
						.dimensions("key1", "key2")
						.measures("metric1", "metric2", "metric3")
						.eq("key1", 1)
						.eq("key2", 3))
				.streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(new DataItemResult(1, 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	private static final int LISTEN_PORT = 45578;

	private EventloopService prepareServer(Eventloop eventloop, Path serverStorage) throws IOException {
		final ExecutorService executor = Executors.newCachedThreadPool();
		SimpleFsServer fileServer = SimpleFsServer.build(eventloop, executor, serverStorage)
				.listenPort(LISTEN_PORT)
				.build();

		fileServer.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Started server");
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to start server", e);
			}
		});
		return fileServer;
	}

	private void stop(EventloopService server) {
		server.stop(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Server has been stopped");
			}

			@Override
			public void onException(Exception exception) {
				logger.info("Failed to stop server");
			}
		});
	}

	@Test
	public void testSimpleFsAggregationStorage() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		final Eventloop eventloop = new Eventloop();

		AggregationStructure aggregationStructure = cubeStructure(classLoader);

		Path serverStorage = temporaryFolder.newFolder("storage").toPath();
		final EventloopService simpleFsServer1 = prepareServer(eventloop, serverStorage);

		AggregationChunkStorage storage = new SimpleFsChunkStorage(eventloop, aggregationStructure,
				new InetSocketAddress(InetAddress.getLocalHost(), LISTEN_PORT));
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);

		final int consumers = 2;
		final CompletionCallback allConsumersDoneCallback = waitAll(consumers, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Streaming to SimpleFS succeeded.");
				stop(simpleFsServer1);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Streaming to SimpleFS failed.", exception);
				stop(simpleFsServer1);
			}
		});

		final StreamConsumer<DataItem1> cubeConsumer1 = cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube, allConsumersDoneCallback));
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cubeConsumer1);

		final StreamConsumer<DataItem2> cubeConsumer2 = cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube, allConsumersDoneCallback));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cubeConsumer2);

		eventloop.run();

		final EventloopService simpleFsServer2 = prepareServer(eventloop, serverStorage);
		final StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		final CubeQuery query = new CubeQuery()
				.dimensions("key1", "key2")
				.measures("metric1", "metric2", "metric3")
				.eq("key1", 1)
				.eq("key2", 3);
		StreamProducer<DataItemResult> queryResultProducer = cube.query(DataItemResult.class, query);
		queryResultProducer.streamTo(consumerToList);
		consumerToList.setCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Streaming query {} result from SimpleFS succeeded.", query);
				stop(simpleFsServer2);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Exception thrown while streaming query {} result from SimpleFS.", query, e);
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
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure aggregationStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 30, 25), new DataItem1(1, 3, 40, 10),
				new DataItem1(1, 4, 23, 48), new DataItem1(1, 3, 4, 18)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 15, 5), new DataItem2(1, 4, 55, 20),
				new DataItem2(1, 2, 12, 42), new DataItem2(1, 4, 58, 22)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult.class,
				new CubeQuery()
						.dimensions("key1", "key2")
						.measures("metric1", "metric2", "metric3")
						.orderAsc("metric2")
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
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure aggregationStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 3, 30, 25), new DataItem1(1, 4, 40, 10),
				new DataItem1(1, 5, 23, 48), new DataItem1(1, 6, 4, 18)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 7, 15, 5), new DataItem2(1, 8, 55, 20),
				new DataItem2(1, 9, 12, 42), new DataItem2(1, 10, 58, 22)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult.class,
				new CubeQuery()
						.dimensions("key1", "key2")
						.measures("metric1", "metric2", "metric3")
						.orderDesc("metric1")
						.orderAsc("metric2")
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(
				new DataItemResult(1, 4, 40, 10, 0),   // metric1 = 40, metric2 = 10
				new DataItemResult(1, 3, 30, 25, 0),   // metric1 = 30, metric2 = 25
				new DataItemResult(1, 5, 23, 48, 0),   // metric1 = 23, metric2 = 48
				new DataItemResult(1, 6, 4, 18, 0),   // metric1 =  4, metric2 = 18
				new DataItemResult(1, 9, 0, 12, 42),   // metric1 =  0, metric2 = 12
				new DataItemResult(1, 7, 0, 15, 5),   // metric1 =  0, metric2 = 15
				new DataItemResult(1, 8, 0, 55, 20),   // metric1 =  0, metric2 = 55
				new DataItemResult(1, 10, 0, 58, 22));  // metric1 =  0, metric2 = 58

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testBetweenPredicate() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure aggregationStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(14, 1, 30, 25),
				new DataItem1(13, 3, 40, 10),
				new DataItem1(9, 4, 23, 48),
				new DataItem1(6, 3, 4, 18),
				new DataItem1(10, 5, 22, 16),
				new DataItem1(20, 7, 13, 49),
				new DataItem1(15, 9, 11, 12),
				new DataItem1(5, 99, 40, 36)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem2(9, 3, 15, 5),
				new DataItem2(11, 4, 55, 20),
				new DataItem2(17, 2, 12, 42),
				new DataItem2(11, 4, 58, 22),
				new DataItem2(19, 18, 22, 55),
				new DataItem2(7, 14, 28, 6),
				new DataItem2(8, 42, 33, 17),
				new DataItem2(5, 77, 88, 98)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult.class,
				new CubeQuery()
						.dimensions("key1", "key2")
						.measures("metric1", "metric2", "metric3")
						.between("key1", 5, 10)
						.between("key2", 40, 1000)
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
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure aggregationStructure = sophisticatedCubeStructure(classLoader);
		Cube cube = newSophisticatedCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem3(14, 1, 42, 25, 53, 30, 25),
				new DataItem3(13, 3, 49, 13, 50, 40, 10),
				new DataItem3(9, 4, 59, 17, 79, 23, 48),
				new DataItem3(6, 3, 30, 20, 63, 4, 18),
				new DataItem3(10, 5, 33, 21, 69, 22, 16),
				new DataItem3(20, 7, 39, 29, 65, 13, 49),
				new DataItem3(15, 9, 57, 26, 59, 11, 12),
				new DataItem3(5, 99, 35, 27, 76, 40, 36)))
				.streamTo(cube.consumer(DataItem3.class, DataItem3.DIMENSIONS, DataItem3.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(
				new DataItem4(9, 3, 41, 11, 65, 15, 5),
				new DataItem4(11, 4, 38, 10, 68, 55, 20),
				new DataItem4(17, 2, 40, 15, 52, 12, 42),
				new DataItem4(11, 4, 47, 22, 60, 58, 22),
				new DataItem4(19, 18, 52, 24, 80, 22, 55),
				new DataItem4(7, 14, 31, 14, 73, 28, 6),
				new DataItem4(8, 42, 46, 19, 75, 33, 17),
				new DataItem4(5, 77, 50, 20, 56, 88, 98)))
				.streamTo(cube.consumer(DataItem4.class, DataItem4.DIMENSIONS, DataItem4.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult3> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult3.class,
				new CubeQuery()
						.dimensions("key1", "key2", "key3", "key4", "key5")
						.measures("metric1", "metric2", "metric3")
						.eq("key1", 5)
						.between("key2", 75, 99)
						.between("key3", 35, 50)
						.eq("key4", 20)
						.eq("key5", 56)
		).streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult3> actual = consumerToList.getList();
		List<DataItemResult3> expected = Collections.singletonList(new DataItemResult3(5, 77, 50, 20, 56, 0, 88, 98));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testGrouping() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure aggregationStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20),
				new DataItem1(1, 2, 15, 25), new DataItem1(1, 1, 95, 85), new DataItem1(2, 1, 55, 65),
				new DataItem1(1, 4, 5, 35)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 20, 10), new DataItem2(1, 4, 10, 20),
				new DataItem2(1, 1, 80, 75)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult2> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult2.class,
				new CubeQuery()
						.dimensions("key2")
						.measures("metric1", "metric2", "metric3"))
				.streamTo(consumerToList);
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
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		ExecutorService executorService = newSingleThreadExecutor();
		Path dir = temporaryFolder.newFolder().toPath();
		AggregationStructure aggregationStructure = cubeStructure(classLoader);
		AggregationChunkStorage storage = new LocalFsChunkStorage(eventloop, executorService, aggregationStructure, dir);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, aggregationStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult.class,
				new CubeQuery()
						.dimensions("key1", "key2")
						.measures("metric1", "metric2", "metric3")
						.eq("key1", 1)
						.eq("key2", 3))
				.streamTo(consumerToList);
		eventloop.run();

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(new DataItemResult(1, 3, 10, 30, 20));

		System.out.println(consumerToList.getList());

		assertEquals(expected, actual);
	}

	@Test
	public void testConsolidate() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		Eventloop eventloop = new Eventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure cubeStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, Executors.newCachedThreadPool(), classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		ResultCallbackFuture<Boolean> future = new ResultCallbackFuture<>();
		cube.consolidate(100, future);

		eventloop.run();
		assertEquals(true, future.get());

		future = new ResultCallbackFuture<>();
		cube.consolidate(100, future);

		eventloop.run();
		assertEquals(true, future.get());

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(DataItemResult.class,
				new CubeQuery()
						.dimensions("key1", "key2")
						.measures("metric1", "metric2", "metric3")
						.eq("key1", 1)
						.eq("key2", 4))
				.streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());

		List<DataItemResult> actual = consumerToList.getList();
		List<DataItemResult> expected = asList(new DataItemResult(1, 4, 0, 30, 60));

		assertEquals(expected, actual);
	}

	public static class MyCommitCallback implements ResultCallback<Multimap<AggregationMetadata, AggregationChunk.NewChunk>> {
		private final Cube cube;
		private final CompletionCallback callback;

		public MyCommitCallback(Cube cube) {
			this(cube, null);
		}

		public MyCommitCallback(Cube cube, CompletionCallback callback) {
			this.cube = cube;
			this.callback = callback;
		}

		@Override
		public void onResult(Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks) {
			cube.incrementLastRevisionId();
			for (Map.Entry<AggregationMetadata, AggregationChunk.NewChunk> entry : newChunks.entries()) {
				AggregationMetadata aggregation = entry.getKey();
				AggregationChunk.NewChunk newChunk = entry.getValue();
				aggregation.addToIndex(createChunk(cube.getLastRevisionId(), newChunk));
			}

			if (callback != null)
				callback.onComplete();
		}

		@Override
		public void onException(Exception exception) {
			logger.error("Exception thrown while trying to commit to cube {}.", cube);

			if (callback != null)
				callback.onException(exception);
		}
	}
}