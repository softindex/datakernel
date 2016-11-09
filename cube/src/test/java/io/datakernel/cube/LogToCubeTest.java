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

import io.datakernel.aggregation.AggregationChunkStorageStub;
import io.datakernel.aggregation.CubeMetadataStorageStub;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.LocalFsLogFileSystem;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogManagerImpl;
import io.datakernel.logfs.LogToCubeRunner;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.Cube.AggregationScheme.id;
import static io.datakernel.cube.TestUtils.deleteRecursivelyQuietly;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class LogToCubeTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testStubStorage() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();
		CubeMetadataStorageStub cubeMetadataStorage = new CubeMetadataStorageStub();
		AggregationChunkStorageStub aggregationStorage = new AggregationChunkStorageStub(eventloop);
		LogToCubeMetadataStorageStub logToCubeMetadataStorageStub = new LogToCubeMetadataStorageStub(cubeMetadataStorage);

		Cube cube = Cube.create(eventloop, executor, classLoader, cubeMetadataStorage, aggregationStorage)
				.withDimension("pub", FieldTypes.ofInt())
				.withDimension("adv", FieldTypes.ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		Path dir = temporaryFolder.newFolder().toPath();
		deleteRecursivelyQuietly(dir);
		LocalFsLogFileSystem fileSystem = LocalFsLogFileSystem.create(eventloop, executor, dir);
		BufferSerializer<TestPubRequest> bufferSerializer = SerializerBuilder
				.create(classLoader)
				.build(TestPubRequest.class);

		LogManager<TestPubRequest> logManager = LogManagerImpl.create(eventloop, fileSystem, bufferSerializer);

		LogToCubeRunner<TestPubRequest> logToCubeRunner = LogToCubeRunner.create(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
				"testlog", asList("partitionA"), logToCubeMetadataStorageStub);

		new StreamProducers.OfIterator<>(eventloop, asList(
				new TestPubRequest(1000, 1, asList(new TestPubRequest.TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestPubRequest.TestAdvRequest(10), new TestPubRequest.TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestPubRequest.TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.<TestPubRequest.TestAdvRequest>asList())).iterator())
				.streamTo(logManager.consumer("partitionA"));

		eventloop.run();

		logToCubeRunner.processLog(IgnoreCompletionCallback.create());

		eventloop.run();

		cube.loadChunks(IgnoreCompletionCallback.create());

		eventloop.run();

		StreamConsumers.ToList<TestAdvResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.queryRawStream(asList("adv"), asList("advRequests"), alwaysTrue(),
				TestAdvResult.class, classLoader
		).streamTo(consumerToList);
		eventloop.run();

		List<TestAdvResult> actualResults = consumerToList.getList();
		List<TestAdvResult> expectedResults = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		System.out.println(consumerToList.getList());

		assertEquals(expectedResults, actualResults);
	}

	public static final class TestPubResult {
		public int pub;
		public long pubRequests;

		@Override
		public String toString() {
			return "TestResult{pub=" + pub + ", pubRequests=" + pubRequests + '}';
		}
	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

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
