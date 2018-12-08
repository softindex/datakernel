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
import io.datakernel.ot.*;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
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
import java.util.stream.Stream;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.datakernel.test.TestUtils.dataSource;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.*;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

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
	private AggregationChunkStorage<Long> aggregationChunkStorage;
	private BinarySerializer<LogItem> serializer;
	private Multilog<LogItem> multilog;

	@Before
	public void before() throws IOException {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		executor = Executors.newCachedThreadPool();
		classLoader = DefiningClassLoader.create();
		dataSource = dataSource("test.properties");
		aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, executor, aggregationsDir));
		serializer = SerializerBuilder.create(classLoader).build(LogItem.class);
		multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, newSingleThreadExecutor(), logsDir),
				serializer,
				NAME_PARTITION_REMAINDER_SEQ);
	}

	@Test
	public void test() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		AggregationChunkStorage<Long> aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, executor, aggregationsDir));
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
		repository.initialize();
		repository.truncateTables();
		repository.createCommitId().thenCompose(id -> repository.push(OTCommit.ofRoot(id)).thenCompose($ -> repository.saveSnapshot(id, emptyList())));
		eventloop.run();

		Multilog<LogItem> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, newSingleThreadExecutor(), logsDir),
				SerializerBuilder.create(classLoader).build(LogItem.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTAlgorithms<Long, LogDiff<CubeDiff>> algorithms = OTAlgorithms.create(eventloop, otSystem, repository);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, algorithms, cubeDiffLogOTState);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop, multilog,
				cube.logStreamConsumer(LogItem.class), "testlog", asList("partitionA"), cubeDiffLogOTState);

		// checkout first (root) revision

		CompletableFuture<?> future;

		future = logCubeStateManager.checkout().toCompletableFuture();
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems1 = LogItem.getListOfRandomLogItems(100);
		StreamSupplier.ofIterable(listOfRandomLogItems1).streamTo(
				multilog.writer("partitionA"));
		eventloop.run();

		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager1 = logCubeStateManager;
		future = logOTProcessor.processLog()
				.thenCompose(logDiff -> aggregationChunkStorage
						.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).map(id -> (long) id).collect(toSet()))
						.thenApply($ -> logDiff))
				.whenResult(logCubeStateManager::add)
				.thenApply($ -> finalLogCubeStateManager1)
				.thenCompose(OTStateManager::commitAndPush)
				.toCompletableFuture();
		eventloop.run();
		future.get();

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
		logCubeStateManager = OTStateManager.create(eventloop, algorithms, cubeDiffLogOTState1);

		logOTProcessor = LogOTProcessor.create(eventloop, multilog, cube.logStreamConsumer(LogItem.class),
				"testlog", asList("partitionA"), cubeDiffLogOTState1);

		future = logCubeStateManager.checkout().toCompletableFuture();
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		StreamSupplier.ofIterable(listOfRandomLogItems2).streamTo(
				multilog.writer("partitionA"));
		eventloop.run();

		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager = logCubeStateManager;
		LogOTProcessor<LogItem, CubeDiff> finalLogOTProcessor = logOTProcessor;
		future = finalLogCubeStateManager.pull()
				.thenCompose($ -> finalLogOTProcessor.processLog())
				.thenCompose(logDiff -> aggregationChunkStorage
						.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).map(id -> (long) id).collect(toSet()))
						.thenApply($ -> logDiff))
				.whenResult(logCubeStateManager::add)
				.thenApply($ -> finalLogCubeStateManager)
				.thenCompose(OTStateManager::commitAndPush)
				.toCompletableFuture();
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
		Map<Integer, Long> map = Stream.concat(listOfRandomLogItems1.stream(), listOfRandomLogItems2.stream())
				.collect(groupingBy(o -> o.date, reducing(0L, o -> o.clicks, (v, v2) -> v + v2)));

		StreamConsumerToList<LogItem> queryResultConsumer2 = StreamConsumerToList.create();
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, classLoader).streamTo(
				queryResultConsumer2);
		eventloop.run();

		// Check query results
		List<LogItem> queryResult2 = queryResultConsumer2.getList();
		for (LogItem logItem : queryResult2) {
			assertEquals(logItem.clicks, map.remove(logItem.date).longValue());
		}
		assertTrue(map.isEmpty());

		// Consolidate
		CompletableFuture<CubeDiff> future1 = cube.consolidate(Aggregation::consolidateHotSegment)
				.thenCompose(cubeDiff -> aggregationChunkStorage.finish(cubeDiff.addedChunks().map(id -> (long) id).collect(toSet()))
						.thenApply($ -> cubeDiff))
				.toCompletableFuture();
		eventloop.run();
		CubeDiff consolidatingCubeDiff = future1.get();
		assertFalse(consolidatingCubeDiff.isEmpty());

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
		StreamConsumerToList<LogItem> queryResultConsumer3 = StreamConsumerToList.create();
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, DefiningClassLoader.create(classLoader))
				.streamTo(queryResultConsumer3);
		eventloop.run();
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
			repository.initialize();
			repository.truncateTables();
			repository.createCommitId().thenCompose(id -> repository.push(OTCommit.ofRoot(id)).thenCompose($ -> repository.saveSnapshot(id, emptyList())));
			eventloop.run();

			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
			OTAlgorithms<Long, LogDiff<CubeDiff>> algorithms = OTAlgorithms.create(eventloop, otSystem, repository);
			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(eventloop, algorithms, cubeDiffLogOTState);

			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);
			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			logCubeStateManager1.checkout();
			eventloop.run();

			StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
					multilog.writer("partitionA"));
			eventloop.run();

			logOTProcessor1.processLog()
					.thenCompose(logDiff -> aggregationChunkStorage
							.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).map(id -> (long) id).collect(toSet()))
							.thenApply($ -> logDiff))
					.whenResult(logCubeStateManager1::add)
					.thenApply($ -> logCubeStateManager1)
					.thenCompose(OTStateManager::commitAndPush);
			eventloop.run();
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

		CompletableFuture<OTCommit<Long, LogDiff<CubeDiff>>> future = otSourceSql2
				.getHeads()
				.thenCompose(newHeads -> {
					assertEquals(1, newHeads.size());
					return otSourceSql2.loadCommit(first(newHeads));
				})
				.toCompletableFuture();

		eventloop.run();
		future.get();
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
			repository.initialize();
			repository.truncateTables();
			repository.createCommitId().thenCompose(id -> repository.push(OTCommit.ofRoot(id)).thenCompose($ -> repository.saveSnapshot(id, emptyList())));
			eventloop.run();

			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
			OTAlgorithms<Long, LogDiff<CubeDiff>> algorithms = OTAlgorithms.create(eventloop, otSystem, repository);
			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(eventloop, algorithms, cubeDiffLogOTState);

			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);

			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			logCubeStateManager1.checkout();
			eventloop.run();

			StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
					multilog.writer("partitionA"));
			eventloop.run();

			logOTProcessor1.processLog()
					.thenCompose(logDiff -> aggregationChunkStorage
							.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).map(id -> (long) id).collect(toSet()))
							.thenApply($ -> logDiff))
					.whenResult(logCubeStateManager1::add)
					.thenApply($ -> logCubeStateManager1)
					.thenCompose(OTStateManager::commitAndPush);
			eventloop.run();
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

		CompletableFuture<OTCommit<Long, LogDiff<CubeDiff>>> future = otSourceSql2
				.getHeads()
				.thenCompose(newHeads -> {
					assertEquals(1, newHeads.size());
					return otSourceSql2.loadCommit(first(newHeads));
				})
				.toCompletableFuture();

		eventloop.run();
		future.get();
	}
}
