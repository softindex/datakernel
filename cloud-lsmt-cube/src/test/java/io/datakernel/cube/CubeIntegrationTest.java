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

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.ChunkIdCodec;
import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeDiffCodec;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.etl.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.multilog.Multilog;
import io.datakernel.multilog.MultilogImpl;
import io.datakernel.ot.*;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.TestUtils.initializeRepository;
import static io.datakernel.cube.TestUtils.runProcessLogs;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.dataSource;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeIntegrationTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@SuppressWarnings({"ConstantConditions", "unchecked", "rawtypes"})
	@Test
	public void test() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Executor executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		RemoteFsChunkStorage<Long> aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, aggregationsDir));
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withDimension("advertiser", ofInt())
				.withDimension("campaign", ofInt())
				.withDimension("banner", ofInt())
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign")
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()))
				.withAggregation(id("detailed")
						.withDimensions("date", "advertiser", "campaign", "banner")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("advertiser")
						.withDimensions("advertiser")
						.withMeasures("impressions", "clicks", "conversions", "revenue"));

		DataSource dataSource = dataSource("test.properties");
		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, LogDiffCodec.create(CubeDiffCodec.create(cube)));
		initializeRepository(repository);

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTNodeImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> node = OTNodeImpl.create(repository, otSystem);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, otSystem, node, cubeDiffLogOTState);

		Multilog<LogItem> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, logsDir),
				SerializerBuilder.create(classLoader).build(LogItem.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				multilog,
				cube.logStreamConsumer(LogItem.class),
				"testlog",
				asList("partitionA"),
				cubeDiffLogOTState);

		// checkout first (root) revision
		await(logCubeStateManager.checkout());

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		await(StreamSupplier.ofIterable(listOfRandomLogItems).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));
		Files.list(logsDir).forEach(System.out::println);

		//		AsynchronousFileChannel channel = AsynchronousFileChannel.open(Files.list(logsDir).findFirst().get(),
		//				EnumSet.of(StandardOpenOption.WRITE), executor);
		//		channel.truncate(13);
		//		channel.write(ByteBuffer.wrap(new byte[]{123}), 0).get();
		//		channel.close();

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(300);
		await(StreamSupplier.ofIterable(listOfRandomLogItems2).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));
		Files.list(logsDir).forEach(System.out::println);

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<LogItem> listOfRandomLogItems3 = LogItem.getListOfRandomLogItems(50);
		await(StreamSupplier.ofIterable(listOfRandomLogItems3).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));
		Files.list(logsDir).forEach(System.out::println);

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		await(aggregationChunkStorage.backup("backup1", (Set) cube.getAllChunks()));

		List<LogItem> logItems = await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader))
				.toList());

		// Aggregate manually
		Map<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);
		aggregateToMap(map, listOfRandomLogItems3);

		// Check query results
		assertEquals(map, logItems.stream().collect(toMap(r -> r.date, r -> r.clicks)));

		// Consolidate revision 4 as revision 5:
		CubeDiff consolidatingCubeDiff = await(cube.consolidate(Aggregation::consolidateHotSegment));
		assertFalse(consolidatingCubeDiff.isEmpty());

		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		await(logCubeStateManager.sync());

		await(aggregationChunkStorage.finish(consolidatingCubeDiff.addedChunks().map(id -> (long) id).collect(toSet())));
		await(aggregationChunkStorage.cleanup((Set) cube.getAllChunks()));

		// Query
		List<LogItem> queryResult = await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).toList());

		assertEquals(map, queryResult.stream().collect(toMap(r -> r.date, r -> r.clicks)));

		// Check files in aggregations directory
		Set<String> actualChunkFileNames = new TreeSet<>();
		for (File file : aggregationsDir.toFile().listFiles()) {
			actualChunkFileNames.add(file.getName());
		}
		assertEquals(concat(Stream.of("backups"), cube.getAllChunks().stream().map(n -> n + ".log")).collect(toSet()),
				actualChunkFileNames);
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
