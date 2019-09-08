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

import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ChunkIdCodec;
import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.cube.bean.TestPubRequest.TestAdvRequest;
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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.TestUtils.initializeRepository;
import static io.datakernel.cube.TestUtils.runProcessLogs;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.dataSource;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public final class LogToCubeTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testStubStorage() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Executor executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		AggregationChunkStorage<Long> aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, aggregationsDir));
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		DataSource dataSource = dataSource("test.properties");
		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, LogDiffCodec.create(CubeDiffCodec.create(cube)));
		initializeRepository(repository);

		List<TestAdvResult> expected = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTNodeImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> node = OTNodeImpl.create(repository, otSystem);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, otSystem, node, cubeDiffLogOTState);

		Multilog<TestPubRequest> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, logsDir),
				SerializerBuilder.create(classLoader).build(TestPubRequest.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<TestPubRequest, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				multilog,
				new TestAggregatorSplitter(cube), // TestAggregatorSplitter.create(eventloop, cube),
				"testlog",
				asList("partitionA"),
				cubeDiffLogOTState);

		StreamSupplier<TestPubRequest> supplier = StreamSupplier.of(
				new TestPubRequest(1000, 1, asList(new TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestAdvRequest(10), new TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.asList()));

		await(supplier.streamTo(StreamConsumer.ofPromise(multilog.write("partitionA"))));
		await(logCubeStateManager.checkout());
		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<TestAdvResult> list = await(cube.queryRawStream(
				asList("adv"),
				asList("advRequests"),
				alwaysTrue(),
				TestAdvResult.class, classLoader)
				.toList());

		assertEquals(expected, list);
	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

		@SuppressWarnings("unused")
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

		@SuppressWarnings("RedundantIfStatement")
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
