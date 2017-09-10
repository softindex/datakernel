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
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.CubeTestUtils.dataSource;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeIntegrationTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@SuppressWarnings({"ConstantConditions", "unchecked"})
	@Test
	public void test() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		LocalFsChunkStorage aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executor, new IdGeneratorStub(), aggregationsDir)
				.withCleanupTimeout(0L);
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withDimension("advertiser", FieldTypes.ofInt())
				.withDimension("campaign", FieldTypes.ofInt())
				.withDimension("banner", FieldTypes.ofInt())
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
		OTRemoteSql<LogDiff<CubeDiff>> otSourceSql = OTRemoteSql.create(executor, dataSource, LogDiffJson.create(CubeDiffJson.create(cube)));
		otSourceSql.truncateTables();
		otSourceSql.push(OTCommit.ofRoot(1));

		OTStateManager<Integer, LogDiff<CubeDiff>> logCubeStateManager = new OTStateManager<>(eventloop,
				LogOT.createLogOT(CubeOT.createCubeOT()),
				otSourceSql,
				(o1, o2) -> Integer.compare(o1, o2),
				new LogOTState<>(cube));

		LogManager<LogItem> logManager = LogManagerImpl.create(eventloop,
				LocalFsLogFileSystem.create(executor, logsDir),
				SerializerBuilder.create(classLoader).build(LogItem.class));

		LogOTProcessor<Integer, LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				logManager,
				cube.logStreamConsumer(LogItem.class),
				"testlog",
				asList("partitionA"),
				logCubeStateManager);

		// checkout first (root) revision

		CompletableFuture<?> future;

		future = logCubeStateManager.checkout().toCompletableFuture();
		eventloop.run();
		future.get();

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		StreamProducer<LogItem> producerOfRandomLogItems = StreamProducers.ofIterator(eventloop, listOfRandomLogItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumerStream("partitionA"));
		eventloop.run();

		future = logOTProcessor.processLog().toCompletableFuture();
		eventloop.run();
		future.get();

		future = logOTProcessor.processLog().toCompletableFuture();
		eventloop.run();
		future.get();

		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(300);
		producerOfRandomLogItems = StreamProducers.ofIterator(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumerStream("partitionA"));
		eventloop.run();

		future = logOTProcessor.processLog().toCompletableFuture();
		eventloop.run();
		future.get();

		List<LogItem> listOfRandomLogItems3 = LogItem.getListOfRandomLogItems(50);
		producerOfRandomLogItems = StreamProducers.ofIterator(eventloop, listOfRandomLogItems3.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumerStream("partitionA"));
		eventloop.run();

		future = logOTProcessor.processLog().toCompletableFuture();
		eventloop.run();
		future.get();

		future = aggregationChunkStorage.backup("backup1", cube.getAllChunks()).toCompletableFuture();
		eventloop.run();
		future.get();

		StreamConsumers.ToList<LogItem> queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer);
		eventloop.run();

		// Aggregate manually
		Map<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);
		aggregateToMap(map, listOfRandomLogItems3);

		// Check query results
		for (LogItem logItem : queryResultConsumer.getList()) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}

		// checkout revision 3 and consolidate it:
		future = logCubeStateManager.checkout(3).toCompletableFuture();
		eventloop.run();
		future.get();

		CompletableFuture<CubeDiff> future1 = cube.consolidate().toCompletableFuture();
		eventloop.run();
		CubeDiff consolidatingCubeDiff = future1.get();
		assertEquals(false, consolidatingCubeDiff.isEmpty());

		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		future = logCubeStateManager.commitAndPush().toCompletableFuture();
		eventloop.run();
		future.get();

		// merge heads: revision 4, and revision 5 (which is a consolidation of 3)

		future = logCubeStateManager.mergeHeadsAndPush().toCompletableFuture();
		eventloop.run();
		future.get();

		// make a checkpoint and checkout it

		future = logCubeStateManager.makeCheckpointForNode(6).toCompletableFuture();
		eventloop.run();
		future.get();

		future = logCubeStateManager.checkout(7).toCompletableFuture();
		eventloop.run();
		future.get();

		future = aggregationChunkStorage.cleanup(cube.getAllChunks()).toCompletableFuture();
		eventloop.run();
		future.get();

		// Query
		queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(classLoader)).streamTo(queryResultConsumer);
		eventloop.run();

		// Check query results
		assertEquals(map.size(), queryResultConsumer.getList().size());
		for (LogItem logItem : queryResultConsumer.getList()) {
			assertEquals(map.get(logItem.date).longValue(), logItem.clicks);
		}

		// Check files in aggregations directory
		Set<String> actualChunkFileNames = new TreeSet<>();
		for (File file : aggregationsDir.toFile().listFiles()) {
			actualChunkFileNames.add(file.getName());
		}
		Set<String> expectedChunkFileNames = new TreeSet<>();
		Stream.of(7, 8, 9, 10, 11, 12).map(n -> n + ".log").forEach(expectedChunkFileNames::add);
		expectedChunkFileNames.add("backups");
		assertEquals(expectedChunkFileNames, actualChunkFileNames);
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
