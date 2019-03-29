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
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeDiffCodec;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.etl.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.multilog.Multilog;
import io.datakernel.multilog.MultilogImpl;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRepositoryMySql;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.async.TestUtils.await;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.TestUtils.initializeRepository;
import static io.datakernel.cube.TestUtils.runProcessLogs;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.datakernel.test.TestUtils.dataSource;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.*;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@RunWith(DatakernelRunner.class)
public class CubeMeasureRemovalTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	private Eventloop eventloop;
	private Executor executor;
	private DefiningClassLoader classLoader;
	private DataSource dataSource;
	private AggregationChunkStorage<Long> aggregationChunkStorage;
	private Multilog<LogItem> multilog;
	private Path aggregationsDir;
	private Path logsDir;

	@Before
	public void before() throws IOException, SQLException {
		aggregationsDir = temporaryFolder.newFolder().toPath();
		logsDir = temporaryFolder.newFolder().toPath();

		eventloop = Eventloop.getCurrentEventloop();
		executor = Executors.newCachedThreadPool();
		classLoader = DefiningClassLoader.create();
		dataSource = dataSource("test.properties");
		aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, aggregationsDir));
		BinarySerializer<LogItem> serializer = SerializerBuilder.create(classLoader).build(LogItem.class);
		multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, logsDir),
				serializer,
				NAME_PARTITION_REMAINDER_SEQ);
	}

	@Test
	public void test() throws Exception {
		AggregationChunkStorage<Long> aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, aggregationsDir));
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withDimension("advertiser", ofInt())
				.withDimension("campaign", ofInt())
				.withDimension("banner", ofInt())
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
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign");

		DataSource dataSource = dataSource("test.properties");
		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, LogDiffCodec.create(CubeDiffCodec.create(cube)));
		initializeRepository(repository);

		Multilog<LogItem> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, logsDir),
				SerializerBuilder.create(classLoader).build(LogItem.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTAlgorithms<Long, LogDiff<CubeDiff>> algorithms = OTAlgorithms.create(eventloop, otSystem, repository);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, algorithms.getOtSystem(), algorithms.getOtNode(), cubeDiffLogOTState);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop, multilog,
				cube.logStreamConsumer(LogItem.class), "testlog", asList("partitionA"), cubeDiffLogOTState);

		// checkout first (root) revision
		await(logCubeStateManager.checkout());

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems1 = LogItem.getListOfRandomLogItems(100);
		await(StreamSupplier.ofIterable(listOfRandomLogItems1).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));

		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager1 = logCubeStateManager;
		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager1, logOTProcessor);

		List<AggregationChunk> chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Initialize cube with new structure (removed measure)
		cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withDimension("advertiser", ofInt())
				.withDimension("campaign", ofInt())
				.withDimension("banner", ofInt())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()))
				.withAggregation(id("detailed")
						.withDimensions("date", "advertiser", "campaign", "banner")
						.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
				.withAggregation(id("advertiser")
						.withDimensions("advertiser")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign");

		LogOTState<CubeDiff> cubeDiffLogOTState1 = LogOTState.create(cube);
		logCubeStateManager = OTStateManager.create(eventloop, algorithms.getOtSystem(), algorithms.getOtNode(), cubeDiffLogOTState1);

		logOTProcessor = LogOTProcessor.create(eventloop, multilog, cube.logStreamConsumer(LogItem.class),
				"testlog", asList("partitionA"), cubeDiffLogOTState1);

		await(logCubeStateManager.checkout());

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		await(StreamSupplier.ofIterable(listOfRandomLogItems2).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));

		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager = logCubeStateManager;
		LogOTProcessor<LogItem, CubeDiff> finalLogOTProcessor = logOTProcessor;
		await(finalLogCubeStateManager.sync());
		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager, finalLogOTProcessor);

		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertFalse(chunks.get(1).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertTrue(chunks.get(1).getMeasures().contains("revenue"));

		// Aggregate manually
		Map<Integer, Long> map = Stream.concat(listOfRandomLogItems1.stream(), listOfRandomLogItems2.stream())
				.collect(groupingBy(o -> o.date, reducing(0L, o -> o.clicks, (v, v2) -> v + v2)));

		StreamConsumerToList<LogItem> queryResultConsumer2 = StreamConsumerToList.create();
		await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, classLoader).streamTo(
				queryResultConsumer2));

		// Check query results
		List<LogItem> queryResult2 = queryResultConsumer2.getList();
		for (LogItem logItem : queryResult2) {
			assertEquals(logItem.clicks, map.remove(logItem.date).longValue());
		}
		assertTrue(map.isEmpty());

		// Consolidate
		CubeDiff consolidatingCubeDiff = await(cube.consolidate(Aggregation::consolidateHotSegment));
		await(aggregationChunkStorage.finish(consolidatingCubeDiff.addedChunks().map(id -> (long) id).collect(toSet())));
		assertFalse(consolidatingCubeDiff.isEmpty());

		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		await(logCubeStateManager.sync());

		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Query
		StreamConsumerToList<LogItem> queryResultConsumer3 = StreamConsumerToList.create();
		await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, DefiningClassLoader.create(classLoader))
				.streamTo(queryResultConsumer3));
		List<LogItem> queryResult3 = queryResultConsumer3.getList();

		// Check that query results before and after consolidation match
		assertEquals(queryResult2.size(), queryResult3.size());
		for (int i = 0; i < queryResult2.size(); ++i) {
			assertEquals(queryResult2.get(i).date, queryResult3.get(i).date);
			assertEquals(queryResult2.get(i).clicks, queryResult3.get(i).clicks);
		}
	}

	@Test
	public void testNewUnknownMeasureInAggregationDiffOnDeserialization() throws Throwable {
		{
			Cube cube1 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
					.withDimension("date", ofLocalDate())
					.withMeasure("impressions", sum(ofLong()))
					.withMeasure("clicks", sum(ofLong()))
					.withMeasure("conversions", sum(ofLong()))
					.withAggregation(id("date")
							.withDimensions("date")
							.withMeasures("impressions", "clicks", "conversions"));

			LogDiffCodec<CubeDiff> diffCodec1 = LogDiffCodec.create(CubeDiffCodec.create(cube1));
			OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
			OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, diffCodec1);
			initializeRepository(repository);

			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
			OTAlgorithms<Long, LogDiff<CubeDiff>> algorithms = OTAlgorithms.create(eventloop, otSystem, repository);
			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(eventloop, algorithms.getOtSystem(), algorithms.getOtNode(), cubeDiffLogOTState);

			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);
			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			await(logCubeStateManager1.checkout());

			await(StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
					StreamConsumer.ofPromise(multilog.write("partitionA"))));

			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
		}

		// Initialize cube with new structure (remove "clicks" from cube configuration)
		Cube cube2 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions"));

		LogDiffCodec<CubeDiff> diffCodec2 = LogDiffCodec.create(CubeDiffCodec.create(cube2));
		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTRepositoryMySql<LogDiff<CubeDiff>> otSourceSql2 = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, diffCodec2);
		otSourceSql2.initialize();

		exception.expectCause(instanceOf(ParseException.class));
		exception.expectCause(hasProperty("message", equalTo("Unknown fields: [clicks, conversions]")));

		Set<Long> newHeads = await(otSourceSql2.getHeads());
		assertEquals(1, newHeads.size());

		await(otSourceSql2.loadCommit(first(newHeads)));
	}

	@Test
	public void testUnknownAggregation() throws Throwable {
		{
			Cube cube1 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
					.withDimension("date", ofLocalDate())
					.withMeasure("impressions", sum(ofLong()))
					.withMeasure("clicks", sum(ofLong()))
					.withAggregation(id("date")
							.withDimensions("date")
							.withMeasures("impressions", "clicks"))
					.withAggregation(id("impressionsAggregation")
							.withDimensions("date")
							.withMeasures("impressions"))
					.withAggregation(id("otherAggregation")
							.withDimensions("date")
							.withMeasures("clicks"));

			LogDiffCodec<CubeDiff> diffCodec1 = LogDiffCodec.create(CubeDiffCodec.create(cube1));
			OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
			OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, diffCodec1);
			initializeRepository(repository);

			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
			OTAlgorithms<Long, LogDiff<CubeDiff>> algorithms = OTAlgorithms.create(eventloop, otSystem, repository);
			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(eventloop, algorithms.getOtSystem(), algorithms.getOtNode(), cubeDiffLogOTState);

			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);

			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			await(logCubeStateManager1.checkout());

			await(StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
					StreamConsumer.ofPromise(multilog.write("partitionA"))));

			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
		}

		// Initialize cube with new structure (remove "impressions" aggregation from cube configuration)
		Cube cube2 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks"));

		LogDiffCodec<CubeDiff> diffCodec2 = LogDiffCodec.create(CubeDiffCodec.create(cube2));
		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTRepositoryMySql<LogDiff<CubeDiff>> otSourceSql2 = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, diffCodec2);
		otSourceSql2.initialize();

		exception.expectCause(instanceOf(ParseException.class));
		exception.expectCause(hasProperty("message", equalTo("Unknown aggregation: impressionsAggregation")));

		Set<Long> newHeads = await(otSourceSql2.getHeads());
		assertEquals(1, newHeads.size());
		await(otSourceSql2.loadCommit(first(newHeads)));
	}
}
