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
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeDiffJson;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.LocalFsLogFileSystem;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogManagerImpl;
import io.datakernel.logfs.ot.*;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteSql;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.CubeTestUtils.dataSource;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.*;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeMeasureRemovalTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	private Eventloop eventloop;
	private ExecutorService executor;
	private DefiningClassLoader classLoader;
	private DataSource dataSource;
	private LocalFsLogFileSystem fileSystem;
	private AggregationChunkStorage aggregationChunkStorage;
	private BufferSerializer<LogItem> serializer;
	private OTSystem<LogDiff<CubeDiff>> logOTSystem;

	@Before
	public void before() throws IOException {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		executor = Executors.newCachedThreadPool();
		classLoader = DefiningClassLoader.create();
		dataSource = dataSource("test.properties");
		fileSystem = LocalFsLogFileSystem.create(executor, logsDir);
		aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executor, new IdGeneratorStub(), aggregationsDir);
		serializer = SerializerBuilder.create(classLoader).build(LogItem.class);
		logOTSystem = LogOT.createLogOT(CubeOT.createCubeOT());
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void test() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		AggregationChunkStorage aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executor, new IdGeneratorStub(), aggregationsDir);
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withDimension("advertiser", FieldTypes.ofInt())
				.withDimension("campaign", FieldTypes.ofInt())
				.withDimension("banner", FieldTypes.ofInt())
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
		final OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTRemoteSql<LogDiff<CubeDiff>> otSourceSql = OTRemoteSql.create(executor, dataSource, otSystem, LogDiffJson.create(CubeDiffJson.create(cube)));
		otSourceSql.truncateTables();
		otSourceSql.createId().thenCompose(integer -> otSourceSql.push(OTCommit.ofRoot(integer)));
		eventloop.run();

		LogManager<LogItem> logManager = LogManagerImpl.create(eventloop,
				LocalFsLogFileSystem.create(executor, logsDir),
				SerializerBuilder.create(classLoader).build(LogItem.class));

		final LogOTState<CubeDiff> cubeDiffLogOTState = new LogOTState<>(cube);
		OTStateManager<Integer, LogDiff<CubeDiff>> logCubeStateManager = new OTStateManager<>(eventloop,
				otSystem, otSourceSql, Integer::compare, cubeDiffLogOTState);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop, logManager,
				cube.logStreamConsumer(LogItem.class), "testlog", asList("partitionA"), cubeDiffLogOTState);

		// checkout first (root) revision

		CompletableFuture<?> future;

		future = logCubeStateManager.checkout().toCompletableFuture();
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems1 = LogItem.getListOfRandomLogItems(100);
		StreamProducer<LogItem> producerOfRandomLogItems1 = StreamProducers.ofIterator(eventloop, listOfRandomLogItems1.iterator());
		producerOfRandomLogItems1.streamTo(logManager.consumerStream("partitionA"));
		eventloop.run();

		OTStateManager<Integer, LogDiff<CubeDiff>> finalLogCubeStateManager1 = logCubeStateManager;
		future = logOTProcessor.processLog()
				.thenCompose(logDiff -> aggregationChunkStorage
						.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).collect(toSet()))
						.thenApply($ -> logDiff))
				.thenAccept(logCubeStateManager::add)
				.thenApply(aVoid -> finalLogCubeStateManager1)
				.thenCompose(OTStateManager::commitAndPush).toCompletableFuture();
		eventloop.run();
		future.get();

		List<AggregationChunk> chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Initialize cube with new structure (removed measure)
		cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withDimension("advertiser", FieldTypes.ofInt())
				.withDimension("campaign", FieldTypes.ofInt())
				.withDimension("banner", FieldTypes.ofInt())
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

		final LogOTState<CubeDiff> cubeDiffLogOTState1 = new LogOTState<>(cube);
		logCubeStateManager = new OTStateManager<>(eventloop, otSystem,
				otSourceSql, Integer::compare, cubeDiffLogOTState1);

		logOTProcessor = LogOTProcessor.create(eventloop, logManager, cube.logStreamConsumer(LogItem.class),
				"testlog", asList("partitionA"), cubeDiffLogOTState1);

		future = logCubeStateManager.checkout().toCompletableFuture();
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		StreamProducer<LogItem> producerOfRandomLogItems2 = StreamProducers.ofIterator(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems2.streamTo(logManager.consumerStream("partitionA"));
		eventloop.run();

		OTStateManager<Integer, LogDiff<CubeDiff>> finalLogCubeStateManager = logCubeStateManager;
		LogOTProcessor<LogItem, CubeDiff> finalLogOTProcessor = logOTProcessor;
		future = finalLogCubeStateManager.pull()
				.thenCompose($ -> finalLogOTProcessor.processLog())
				.thenCompose(logDiff -> aggregationChunkStorage
						.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).collect(toSet()))
						.thenApply($ -> logDiff))
				.thenAccept(logCubeStateManager::add)
				.thenApply(aVoid -> finalLogCubeStateManager)
				.thenCompose(OTStateManager::commitAndPush).toCompletableFuture();
		eventloop.run();
		future.get();

		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertFalse(chunks.get(1).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertTrue(chunks.get(1).getMeasures().contains("revenue"));

		// Aggregate manually
		final Map<Integer, Long> map = Stream.concat(listOfRandomLogItems1.stream(), listOfRandomLogItems2.stream())
				.collect(groupingBy(o -> o.date, reducing(0L, o -> o.clicks, (v, v2) -> v + v2)));

		StreamConsumerToList<LogItem> queryResultConsumer2 = new StreamConsumerToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, classLoader)
				.streamTo(queryResultConsumer2);
		eventloop.run();

		// Check query results
		List<LogItem> queryResult2 = queryResultConsumer2.getList();
		for (LogItem logItem : queryResult2) {
			assertEquals(logItem.clicks, map.remove(logItem.date).longValue());
		}
		assertTrue(map.isEmpty());

		// Consolidate
		CompletableFuture<CubeDiff> future1 = cube.consolidate(Aggregation::consolidateHotSegment)
				.thenCompose(cubeDiff -> aggregationChunkStorage.finish(cubeDiff.addedChunks().collect(Collectors.toSet()))
						.thenApply(aVoid -> cubeDiff))
				.toCompletableFuture();
		eventloop.run();
		CubeDiff consolidatingCubeDiff = future1.get();
		assertEquals(false, consolidatingCubeDiff.isEmpty());

		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		future = logCubeStateManager.commitAndPush().toCompletableFuture();
		eventloop.run();
		future.get();

		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Query
		StreamConsumerToList<LogItem> queryResultConsumer3 = new StreamConsumerToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer3);
		eventloop.run();
		List<LogItem> queryResult3 = queryResultConsumer3.getList();

		// Check that query results before and after consolidation match
		assertEquals(queryResult2.size(), queryResult3.size());
		for (int i = 0; i < queryResult2.size(); ++i) {
			assertEquals(queryResult2.get(i).date, queryResult3.get(i).date);
			assertEquals(queryResult2.get(i).clicks, queryResult3.get(i).clicks);
		}
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testNewUnknownMeasureInAggregationDiffOnDeserialization() throws Throwable {
		{
			final Cube cube1 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
					.withDimension("date", FieldTypes.ofLocalDate())
					.withMeasure("impressions", sum(ofLong()))
					.withMeasure("clicks", sum(ofLong()))
					.withMeasure("conversions", sum(ofLong()))
					.withAggregation(id("date")
							.withDimensions("date")
							.withMeasures("impressions", "clicks", "conversions"));

			final LogDiffJson<CubeDiff> diffAdapter1 = LogDiffJson.create(CubeDiffJson.create(cube1));
			final OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
			final OTRemoteSql<LogDiff<CubeDiff>> otSourceSql1 = OTRemoteSql.create(executor, dataSource, otSystem, diffAdapter1);
			otSourceSql1.truncateTables();
			otSourceSql1.createId().thenCompose(integer -> otSourceSql1.push(OTCommit.ofRoot(integer)));
			eventloop.run();

			final LogOTState<CubeDiff> cubeDiffLogOTState = new LogOTState<>(cube1);
			final OTStateManager<Integer, LogDiff<CubeDiff>> logCubeStateManager1 = new OTStateManager<>(eventloop,
					logOTSystem, otSourceSql1, Integer::compare, cubeDiffLogOTState);

			final LogManager<LogItem> logManager = LogManagerImpl.create(eventloop, fileSystem, serializer);

			final LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);
			final LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
					logManager, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			logCubeStateManager1.checkout();
			eventloop.run();

			final List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
			StreamProducers.ofIterable(eventloop, listOfRandomLogItems).streamTo(logManager.consumerStream("partitionA"));
			eventloop.run();

			logOTProcessor1.processLog()
					.thenCompose(logDiff -> aggregationChunkStorage
							.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).collect(toSet()))
							.thenApply($ -> logDiff))
					.thenAccept(logCubeStateManager1::add)
					.thenApply(aVoid -> logCubeStateManager1)
					.thenCompose(OTStateManager::commitAndPush);
			eventloop.run();
		}

		// Initialize cube with new structure (remove "clicks" from cube configuration)
		final Cube cube2 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions"));

		final LogDiffJson<CubeDiff> diffAdapter2 = LogDiffJson.create(CubeDiffJson.create(cube2));
		final OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		final OTRemoteSql<LogDiff<CubeDiff>> otSourceSql2 = OTRemoteSql.create(executor, dataSource, otSystem, diffAdapter2);

		exception.expectCause(hasCause(instanceOf(IOException.class)));
		exception.expectMessage("Unknown fields: [clicks, conversions]");

		final CompletableFuture<OTCommit<Integer, LogDiff<CubeDiff>>> future = otSourceSql2.getHeads()
				.thenCompose(newHeads -> {
					assertEquals(1, newHeads.size());
					return otSourceSql2.loadCommit(newHeads.iterator().next());
				}).toCompletableFuture();

		eventloop.run();
		future.get();
	}

	@Test
	public void testUnknownAggregation() throws Throwable {
		{
			final Cube cube1 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
					.withDimension("date", FieldTypes.ofLocalDate())
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

			final LogDiffJson<CubeDiff> diffAdapter1 = LogDiffJson.create(CubeDiffJson.create(cube1));
			final OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
			final OTRemoteSql<LogDiff<CubeDiff>> otSourceSql1 = OTRemoteSql.create(executor, dataSource, otSystem, diffAdapter1);
			otSourceSql1.truncateTables();
			otSourceSql1.createId().thenCompose(integer -> otSourceSql1.push(OTCommit.ofRoot(integer)));
			eventloop.run();

			final LogOTState<CubeDiff> cubeDiffLogOTState = new LogOTState<>(cube1);
			final OTStateManager<Integer, LogDiff<CubeDiff>> logCubeStateManager1 = new OTStateManager<>(eventloop,
					logOTSystem, otSourceSql1, Integer::compare, cubeDiffLogOTState);

			final LogManager<LogItem> logManager = LogManagerImpl.create(eventloop, fileSystem, serializer);
			final LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);

			final LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
					logManager, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			logCubeStateManager1.checkout();
			eventloop.run();

			final List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
			StreamProducers.ofIterable(eventloop, listOfRandomLogItems).streamTo(logManager.consumerStream("partitionA"));
			eventloop.run();

			logOTProcessor1.processLog()
					.thenCompose(logDiff -> aggregationChunkStorage
							.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).collect(toSet()))
							.thenApply($ -> logDiff))
					.thenAccept(logCubeStateManager1::add)
					.thenApply(aVoid -> logCubeStateManager1)
					.thenCompose(OTStateManager::commitAndPush);
			eventloop.run();
		}

		// Initialize cube with new structure (remove "impressions" aggregation from cube configuration)
		final Cube cube2 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks"));

		final LogDiffJson<CubeDiff> diffAdapter2 = LogDiffJson.create(CubeDiffJson.create(cube2));
		final OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		final OTRemoteSql<LogDiff<CubeDiff>> otSourceSql2 = OTRemoteSql.create(executor, dataSource, otSystem, diffAdapter2);

		exception.expectCause(hasCause(instanceOf(IOException.class)));
		exception.expectMessage("Unknown aggregations: [impressionsAggregation, otherAggregation]");

		final CompletableFuture<OTCommit<Integer, LogDiff<CubeDiff>>> future = otSourceSql2.getHeads()
				.thenCompose(newHeads -> {
					assertEquals(1, newHeads.size());
					return otSourceSql2.loadCommit(newHeads.iterator().next());
				}).toCompletableFuture();

		eventloop.run();
		future.get();
	}
}
