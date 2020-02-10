/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.aggregation.*;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.*;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.datakernel.aggregation.AggregationPredicates.*;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.codegen.DefiningClassLoader.create;
import static io.datakernel.common.collection.CollectionUtils.keysToMap;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
public final class CubeTest {
	private static final int LISTEN_PORT = getFreePort();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final DefiningClassLoader classLoader = create();
	private final Executor executor = newSingleThreadExecutor();

	private AggregationChunkStorage<Long> chunkStorage;
	private Cube cube;

	@Before
	public void setUp() throws Exception {
		LocalFsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), temporaryFolder.newFolder().toPath());
		chunkStorage = RemoteFsChunkStorage.create(Eventloop.getCurrentEventloop(), ChunkIdCodec.ofLong(), new IdGeneratorStub(), storage);
		cube = newCube(executor, classLoader, chunkStorage);
	}

	private static Cube newCube(Executor executor, DefiningClassLoader classLoader, AggregationChunkStorage chunkStorage) {
		return Cube.create(Eventloop.getCurrentEventloop(), executor, classLoader, chunkStorage)
				.withDimension("key1", ofInt())
				.withDimension("key2", ofInt())
				.withMeasure("metric1", sum(ofLong()))
				.withMeasure("metric2", sum(ofLong()))
				.withMeasure("metric3", sum(ofLong()))
				.withAggregation(id("detailedAggregation").withDimensions("key1", "key2").withMeasures("metric1", "metric2", "metric3"));
	}

	private static Cube newSophisticatedCube(Executor executor, DefiningClassLoader classLoader, AggregationChunkStorage chunkStorage) {
		return Cube.create(Eventloop.getCurrentEventloop(), executor, classLoader, chunkStorage)
				.withDimension("key1", ofInt())
				.withDimension("key2", ofInt())
				.withDimension("key3", ofInt())
				.withDimension("key4", ofInt())
				.withDimension("key5", ofInt())
				.withMeasure("metric1", sum(ofLong()))
				.withMeasure("metric2", sum(ofLong()))
				.withMeasure("metric3", sum(ofLong()))
				.withAggregation(id("detailedAggregation").withDimensions("key1", "key2", "key3", "key4", "key5").withMeasures("metric1", "metric2", "metric3"));
	}

	@SuppressWarnings("unchecked")
	private static <T> Promise<Void> consume(Cube cube, AggregationChunkStorage<Long> chunkStorage, T item, T... items) {
		return StreamSupplier.concat(StreamSupplier.of(item), StreamSupplier.of(items))
				.streamTo(cube.consume(((Class<T>) item.getClass())))
				.then(cubeDiff -> chunkStorage.finish(cubeDiff.<Long>addedChunks().collect(toSet()))
						.whenResult($ -> cube.apply(cubeDiff)));
	}

	@Test
	public void testQuery1() throws QueryException {
		List<DataItemResult> expected = singletonList(new DataItemResult(1, 3, 10, 30, 20));

		await(
				consume(cube, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
				consume(cube, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20))
		);
		List<DataItemResult> list = await(cube.queryRawStream(
				asList("key1", "key2"),
				asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, classLoader)
				.toList());

		assertEquals(expected, list);
	}

	private RemoteFsServer startServer(Executor executor, Path serverStorage) throws IOException {
		RemoteFsServer fileServer = RemoteFsServer.create(Eventloop.getCurrentEventloop(), executor, serverStorage)
				.withListenPort(LISTEN_PORT);
		fileServer.listen();
		return fileServer;
	}

	@Test
	public void testRemoteFsAggregationStorage() throws Exception {

		Path serverStorage = temporaryFolder.newFolder("storage").toPath();
		RemoteFsServer remoteFsServer1 = startServer(executor, serverStorage);
		RemoteFsClient storage = RemoteFsClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress("localhost", LISTEN_PORT));
		AggregationChunkStorage<Long> chunkStorage = RemoteFsChunkStorage.create(Eventloop.getCurrentEventloop(), ChunkIdCodec.ofLong(), new IdGeneratorStub(), storage);
		Cube cube = newCube(executor, classLoader, chunkStorage);

		List<DataItemResult> expected = singletonList(new DataItemResult(1, 3, 10, 30, 20));

		await(
				Promises.all(consume(cube, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
						consume(cube, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
						.whenComplete(remoteFsServer1::close)
		);
		RemoteFsServer remoteFsServer2 = startServer(executor, serverStorage);

		List<DataItemResult> list = await(cube.queryRawStream(
				asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, classLoader)
				.toList()
				.whenComplete(remoteFsServer2::close));

		assertEquals(expected, list);
	}

	@Test
	public void testOrdering() throws QueryException {
		List<DataItemResult> expected = asList(
				new DataItemResult(1, 2, 30, 37, 42), // metric2 =  37
				new DataItemResult(1, 3, 44, 43, 5),  // metric2 =  43
				new DataItemResult(1, 4, 23, 161, 42) // metric2 = 161
		);

		await(
				consume(cube, chunkStorage, new DataItem1(1, 2, 30, 25), new DataItem1(1, 3, 40, 10), new DataItem1(1, 4, 23, 48), new DataItem1(1, 3, 4, 18)),
				consume(cube, chunkStorage, new DataItem2(1, 3, 15, 5), new DataItem2(1, 4, 55, 20), new DataItem2(1, 2, 12, 42), new DataItem2(1, 4, 58, 22))
		);
		List<DataItemResult> list = await(cube.queryRawStream(
				asList("key1", "key2"),
				asList("metric1", "metric2", "metric3"),
				alwaysTrue(),
				DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testMultipleOrdering() throws QueryException {
		List<DataItemResult> expected = asList(
				new DataItemResult(1, 3, 30, 25, 0),  // metric1 = 30, metric2 = 25
				new DataItemResult(1, 4, 40, 10, 0),  // metric1 = 40, metric2 = 10
				new DataItemResult(1, 5, 23, 48, 0),  // metric1 = 23, metric2 = 48
				new DataItemResult(1, 6, 4, 18, 0),   // metric1 =  4, metric2 = 18
				new DataItemResult(1, 7, 0, 15, 5),   // metric1 =  0, metric2 = 15
				new DataItemResult(1, 8, 0, 55, 20),  // metric1 =  0, metric2 = 55
				new DataItemResult(1, 9, 0, 12, 42),  // metric1 =  0, metric2 = 12
				new DataItemResult(1, 10, 0, 58, 22)  // metric1 =  0, metric2 = 58
		);

		await(
				consume(cube, chunkStorage, new DataItem1(1, 3, 30, 25), new DataItem1(1, 4, 40, 10), new DataItem1(1, 5, 23, 48), new DataItem1(1, 6, 4, 18)),
				consume(cube, chunkStorage, new DataItem2(1, 7, 15, 5), new DataItem2(1, 8, 55, 20), new DataItem2(1, 9, 12, 42), new DataItem2(1, 10, 58, 22))
		);

		List<DataItemResult> list = await(cube.queryRawStream(
				asList("key1", "key2"),
				asList("metric1", "metric2", "metric3"),
				alwaysTrue(),
				DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testBetweenPredicate() throws QueryException {
		List<DataItemResult> expected = asList(
				new DataItemResult(5, 77, 0, 88, 98),
				new DataItemResult(5, 99, 40, 36, 0),
				new DataItemResult(8, 42, 0, 33, 17)
		);

		await(
				consume(cube, chunkStorage,
						new DataItem1(14, 1, 30, 25), new DataItem1(13, 3, 40, 10), new DataItem1(9, 4, 23, 48), new DataItem1(6, 3, 4, 18),
						new DataItem1(10, 5, 22, 16), new DataItem1(20, 7, 13, 49), new DataItem1(15, 9, 11, 12), new DataItem1(5, 99, 40, 36)),
				consume(cube, chunkStorage, new DataItem1(1, 3, 30, 25), new DataItem1(1, 4, 40, 10), new DataItem1(1, 5, 23, 48), new DataItem1(1, 6, 4, 18)),
				consume(cube, chunkStorage,
						new DataItem2(9, 3, 15, 5), new DataItem2(11, 4, 55, 20), new DataItem2(17, 2, 12, 42), new DataItem2(11, 4, 58, 22),
						new DataItem2(19, 18, 22, 55), new DataItem2(7, 14, 28, 6), new DataItem2(8, 42, 33, 17), new DataItem2(5, 77, 88, 98)),
				consume(cube, chunkStorage, new DataItem2(1, 7, 15, 5), new DataItem2(1, 8, 55, 20), new DataItem2(1, 9, 12, 42), new DataItem2(1, 10, 58, 22))
		);

		List<DataItemResult> list = await(cube.queryRawStream(
				asList("key1", "key2"),
				asList("metric1", "metric2", "metric3"),
				and(between("key1", 5, 10), between("key2", 40, 1000)),
				DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testBetweenTransformation() throws QueryException {
		cube = newSophisticatedCube(executor, classLoader, chunkStorage);

		List<DataItemResult3> expected = singletonList(new DataItemResult3(5, 77, 50, 20, 56, 0, 88, 98));

		await(
				consume(cube, chunkStorage,
						new DataItem3(14, 1, 42, 25, 53, 30, 25), new DataItem3(13, 3, 49, 13, 50, 40, 10), new DataItem3(9, 4, 59, 17, 79, 23, 48),
						new DataItem3(6, 3, 30, 20, 63, 4, 18), new DataItem3(10, 5, 33, 21, 69, 22, 16), new DataItem3(20, 7, 39, 29, 65, 13, 49),
						new DataItem3(15, 9, 57, 26, 59, 11, 12), new DataItem3(5, 99, 35, 27, 76, 40, 36)),
				consume(cube, chunkStorage,
						new DataItem4(9, 3, 41, 11, 65, 15, 5), new DataItem4(11, 4, 38, 10, 68, 55, 20), new DataItem4(17, 2, 40, 15, 52, 12, 42),
						new DataItem4(11, 4, 47, 22, 60, 58, 22), new DataItem4(19, 18, 52, 24, 80, 22, 55), new DataItem4(7, 14, 31, 14, 73, 28, 6),
						new DataItem4(8, 42, 46, 19, 75, 33, 17), new DataItem4(5, 77, 50, 20, 56, 88, 98))
		);

		List<DataItemResult3> list = await(cube.queryRawStream(
				asList("key1", "key2", "key3", "key4", "key5"),
				asList("metric1", "metric2", "metric3"),
				and(eq("key1", 5), between("key2", 75, 99), between("key3", 35, 50), eq("key4", 20), eq("key5", 56)),
				DataItemResult3.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testGrouping() throws QueryException {
		List<DataItemResult2> expected = asList(
				new DataItemResult2(1, 150, 230, 75),
				new DataItemResult2(2, 25, 45, 0),
				new DataItemResult2(3, 10, 40, 10),
				new DataItemResult2(4, 5, 45, 20));

		await(
				consume(cube, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20), new DataItem1(1, 2, 15, 25),
						new DataItem1(1, 1, 95, 85), new DataItem1(2, 1, 55, 65), new DataItem1(1, 4, 5, 35)),
				consume(cube, chunkStorage, new DataItem2(1, 3, 20, 10), new DataItem2(1, 4, 10, 20), new DataItem2(1, 1, 80, 75))
		);
		// SELECT key1, SUM(metric1), SUM(metric2), SUM(metric3) FROM detailedAggregation WHERE key1 = 1 AND key2 = 3 GROUP BY key1

		List<DataItemResult2> list = await(cube.queryRawStream(singletonList("key2"), asList("metric1", "metric2", "metric3"),
				alwaysTrue(),
				DataItemResult2.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testQuery2() throws QueryException {
		List<DataItemResult> expected = singletonList(new DataItemResult(1, 3, 10, 30, 20));

		await(
				consume(cube, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
				consume(cube, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)),
				consume(cube, chunkStorage, new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)),
				consume(cube, chunkStorage, new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200))
		);

		List<DataItemResult> list = await(cube.queryRawStream(asList("key1", "key2"), asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testConsolidate() throws QueryException {
		List<DataItemResult> expected = singletonList(new DataItemResult(1, 4, 0, 30, 60));

		await(
				consume(cube, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
				consume(cube, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)),
				consume(cube, chunkStorage, new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)),
				consume(cube, chunkStorage, new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200))
		);

		CubeDiff diff = await(cube.consolidate(Aggregation::consolidateHotSegment));
		assertFalse(diff.isEmpty());

		diff = await(cube.consolidate(Aggregation::consolidateHotSegment));
		assertFalse(diff.isEmpty());

		List<DataItemResult> list = await(cube.queryRawStream(
				asList("key1", "key2"),
				asList("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 4)),
				DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testAggregationPredicate() {
		AggregationPredicate aggregationPredicate;
		AggregationPredicate query;
		AggregationPredicate intersection;

		aggregationPredicate = AggregationPredicates.alwaysTrue();
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertEquals(intersection, query);

		aggregationPredicate = AggregationPredicates.eq("dimensionA", 1);
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertEquals(intersection, query);

		aggregationPredicate = AggregationPredicates.eq("dimensionA", 1);
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 2), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);

		aggregationPredicate = AggregationPredicates.eq("dimensionA", 1);
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionA"), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);

		aggregationPredicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);

		aggregationPredicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionX", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertEquals(intersection, query);

		aggregationPredicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.and(AggregationPredicates.eq("dimensionX", 1), AggregationPredicates.eq("dimensionB", 2))).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertEquals(intersection, query);

		aggregationPredicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("dimensionX", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertEquals(intersection, query);

		// betweens

		aggregationPredicate = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 100, 200));
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("date", 1)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("date", 150)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("date", 250)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 110, 190)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 10, 90)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);
		assertEquals(intersection, AggregationPredicates.alwaysFalse());

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 210, 290)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);
		assertEquals(intersection, AggregationPredicates.alwaysFalse());

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 10, 290)).simplify();
		intersection = AggregationPredicates.and(query, aggregationPredicate).simplify();
		assertNotEquals(intersection, query);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnknownDimensions() throws IOException {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Executor executor = newSingleThreadExecutor();

		LocalFsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), temporaryFolder.newFolder().toPath());
		AggregationChunkStorage<Long> chunkStorage = RemoteFsChunkStorage.create(Eventloop.getCurrentEventloop(), ChunkIdCodec.ofLong(), new IdGeneratorStub(), storage);
		Cube cube = newCube(executor, classLoader, chunkStorage);

		cube.consume(DataItem1.class,
				keysToMap(Stream.of("unknownKey"), identity()),
				keysToMap(Stream.of("metric1", "metric2", "metric3"), identity()),
				AggregationPredicates.alwaysTrue());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnknownMeasure() throws IOException {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Executor executor = newSingleThreadExecutor();

		LocalFsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), temporaryFolder.newFolder().toPath());
		AggregationChunkStorage<Long> chunkStorage = RemoteFsChunkStorage.create(Eventloop.getCurrentEventloop(), ChunkIdCodec.ofLong(), new IdGeneratorStub(), storage);
		Cube cube = newCube(executor, classLoader, chunkStorage);

		cube.consume(DataItem1.class,
				keysToMap(Stream.of("key1", "key2"), identity()),
				keysToMap(Stream.of("UnknownMetric"), identity()),
				AggregationPredicates.alwaysTrue());
	}

}
