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
import com.google.common.collect.Multimap;
import io.datakernel.asm.DefiningClassLoader;
import io.datakernel.async.CompletionCallback;
import io.datakernel.cube.dimensiontype.DimensionType;
import io.datakernel.cube.dimensiontype.DimensionTypeInt;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
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
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.async.AsyncCallbacks.waitAll;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

public class CubeTest {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CubeTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static Cube newCube(Eventloop eventloop, DefiningClassLoader classLoader, AggregationStorage storage,
	                           CubeStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, new LogToCubeMetadataStorageStub(), storage, cubeStructure);
		cube.addAggregation(
				new Aggregation("detailedAggregation", asList("key1", "key2"), asList("metric1", "metric2", "metric3")));
		cube.addAggregation(new Aggregation("key1", asList("key1"), asList("metric1", "metric2", "metric3")));
		return cube;
	}

	public static Cube newSophisticatedCube(Eventloop eventloop, DefiningClassLoader classLoader, AggregationStorage storage,
	                                        CubeStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, new LogToCubeMetadataStorageStub(), storage, cubeStructure);
		cube.addAggregation(
				new Aggregation("detailedAggregation", asList("key1", "key2", "key3", "key4", "key5"),
						asList("metric1", "metric2", "metric3")));
		return cube;
	}

	public static CubeStructure cubeStructure(DefiningClassLoader classLoader) {
		return new CubeStructure(classLoader,
				ImmutableMap.<String, DimensionType>builder()
						.put("key1", new DimensionTypeInt())
						.put("key2", new DimensionTypeInt())
						.build(),
				ImmutableMap.<String, MeasureType>builder()
						.put("metric1", MeasureType.SUM_LONG)
						.put("metric2", MeasureType.SUM_LONG)
						.put("metric3", MeasureType.SUM_LONG)
						.build());
	}

	public static CubeStructure sophisticatedCubeStructure(DefiningClassLoader classLoader) {
		return new CubeStructure(classLoader,
				ImmutableMap.<String, DimensionType>builder()
						.put("key1", new DimensionTypeInt())
						.put("key2", new DimensionTypeInt())
						.put("key3", new DimensionTypeInt())
						.put("key4", new DimensionTypeInt())
						.put("key5", new DimensionTypeInt())
						.build(),
				ImmutableMap.<String, MeasureType>builder()
						.put("metric1", MeasureType.SUM_LONG)
						.put("metric2", MeasureType.SUM_LONG)
						.put("metric3", MeasureType.SUM_LONG)
						.build());
	}

	@Test
	public void testQuery1() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult.class,
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

	private static final int LISTEN_PORT = 45555;

	private SimpleFsServer prepareServer(NioEventloop eventloop, ExecutorService executor) throws IOException {
		SimpleFsServer fileServer = SimpleFsServer.createServer(eventloop, temporaryFolder.newFolder().toPath(), executor);
		fileServer.setListenPort(LISTEN_PORT);
//		fileServer.acceptOnce();
		try {
			fileServer.listen();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fileServer;
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void testSimpleFsAggregationStorage() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();

		final SimpleFsServer simpleFsServer = prepareServer(eventloop, executor);
		CubeStructure cubeStructure = cubeStructure(classLoader);

		Path aggregationsDir = temporaryFolder.newFolder().toPath();

//		AggregationStorage storage = new SimpleFsAggregationStorage(eventloop, executor, cubeStructure);
		AggregationStorage storage = new LocalFsAggregationStorage(eventloop, executor, cubeStructure, aggregationsDir);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);

		final StreamConsumer<DataItem1> cubeConsumer1 = cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube));
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cubeConsumer1);

		final StreamConsumer<DataItem2> cubeConsumer2 = cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cubeConsumer2);
		final int consumers = 2;

		final CompletionCallback allConsumersDoneCallback = waitAll(consumers, new CompletionCallback() {
			@Override
			public void onComplete() {
				simpleFsServer.close();
			}

			@Override
			public void onException(Exception exception) {
			}
		});

		cubeConsumer1.addCompletionCallback(allConsumersDoneCallback);
		cubeConsumer2.addCompletionCallback(allConsumersDoneCallback);

		eventloop.run();

		simpleFsServer.setListenPort(LISTEN_PORT);
		simpleFsServer.listen();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		final CubeQuery query = new CubeQuery()
				.dimensions("key1", "key2")
				.measures("metric1", "metric2", "metric3")
				.eq("key1", 1)
				.eq("key2", 3);
		StreamProducer<DataItemResult> queryResultProducer = cube.query(0, DataItemResult.class, query);
		queryResultProducer.streamTo(consumerToList);
		queryResultProducer.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				simpleFsServer.close();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Exception thrown while streaming query {} result.", query, e);
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
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 30, 25), new DataItem1(1, 3, 40, 10),
				new DataItem1(1, 4, 23, 48), new DataItem1(1, 3, 4, 18)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 15, 5), new DataItem2(1, 4, 55, 20),
				new DataItem2(1, 2, 12, 42), new DataItem2(1, 4, 58, 22)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult.class,
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
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 3, 30, 25), new DataItem1(1, 4, 40, 10),
				new DataItem1(1, 5, 23, 48), new DataItem1(1, 6, 4, 18)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 7, 15, 5), new DataItem2(1, 8, 55, 20),
				new DataItem2(1, 9, 12, 42), new DataItem2(1, 10, 58, 22)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult.class,
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
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
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

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult.class,
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
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = sophisticatedCubeStructure(classLoader);
		Cube cube = newSophisticatedCube(eventloop, classLoader, storage, cubeStructure);
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

		StreamConsumers.ToList<DataItemResult3> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult3.class,
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
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20),
				new DataItem1(1, 2, 15, 25), new DataItem1(1, 1, 95, 85), new DataItem1(2, 1, 55, 65),
				new DataItem1(1, 4, 5, 35)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 20, 10), new DataItem2(1, 4, 10, 20),
				new DataItem2(1, 1, 80, 75)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult2> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult2.class,
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
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executorService = newSingleThreadExecutor();
		Path dir = temporaryFolder.newFolder().toPath();
		Utils.deleteRecursivelyQuietly(dir);
		CubeStructure cubeStructure = cubeStructure(classLoader);
		AggregationStorage storage = new LocalFsAggregationStorage(eventloop, executorService, cubeStructure, dir);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult.class,
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
		NioEventloop eventloop = new NioEventloop();
		AggregationStorageStub storage = new AggregationStorageStub(eventloop, classLoader);
		CubeStructure cubeStructure = cubeStructure(classLoader);
		Cube cube = newCube(eventloop, classLoader, storage, cubeStructure);
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new MyCommitCallback(cube)));
		eventloop.run();

		cube.consolidate(new MyConsolidateCallback(cube));

		eventloop.run();

		cube.consolidate(new MyConsolidateCallback(cube));

		eventloop.run();

		StreamConsumers.ToList<DataItemResult> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);
		cube.query(0, DataItemResult.class,
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

	public static class MyCommitCallback implements CommitCallback {
		private final Cube cube;

		public MyCommitCallback(Cube cube) {
			this.cube = cube;
		}

		@Override
		public void onCommit(Multimap<Aggregation, AggregationChunk.NewChunk> newChunks) {
			cube.incrementLastRevisionId();
			for (Map.Entry<Aggregation, AggregationChunk.NewChunk> entry : newChunks.entries()) {
				Aggregation aggregation = entry.getKey();
				AggregationChunk.NewChunk newChunk = entry.getValue();
				aggregation.addToIndex(AggregationChunk.createCommitChunk(cube.getLastRevisionId(), newChunk));
			}
		}

		@Override
		public void onException(Exception exception) {
			logger.error("Exception thrown while trying to commit to cube {}.", cube);
		}
	}

	private static class MyConsolidateCallback implements ConsolidateCallback {
		private final Cube cube;

		public MyConsolidateCallback(Cube cube) {
			this.cube = cube;
		}

		@Override
		public void onConsolidate(Aggregation aggregation, List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks) {
			cube.incrementLastRevisionId();
			for (AggregationChunk originalChunk : originalChunks) {
				aggregation.removeFromIndex(originalChunk);
			}
			for (AggregationChunk.NewChunk consolidatedChunk : consolidatedChunks) {
				aggregation.addToIndex(AggregationChunk.createConsolidateChunk(cube.getLastRevisionId(), originalChunks, consolidatedChunk));
			}
		}

		@Override
		public void onNothingToConsolidate() {
			logger.trace("Nothing to consolidate in cube {}.", cube);
		}

		@Override
		public void onException(Exception exception) {
			logger.error("Exception thrown while performing cube {} consolidation.", cube);
		}
	}
}