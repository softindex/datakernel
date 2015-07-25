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
import com.google.common.reflect.TypeToken;
import io.datakernel.async.CompletionCallbackObserver;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.LogToCubeTest.TestAdvResult;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.logfs.LogFileSystemImpl;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogManagerImpl;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerFactory;
import io.datakernel.serializer.SerializerScanner;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.cube.LogToCubeTest.getStructure;
import static io.datakernel.cube.LogToCubeTest.newCube;
import static io.datakernel.cube.Utils.deleteRecursivelyQuietly;
import static java.util.Arrays.asList;

public class CubeBenchmark {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(CubeBenchmark.class);
	private static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

	public static void main(String[] args) {
		StreamConsumers.ToList<TestAdvResult> consumerToList;
		int iterations = 30;
		root.setLevel(Level.ERROR);
		DefiningClassLoader classLoader = new DefiningClassLoader();
		ExecutorService executor = Executors.newCachedThreadPool();
		Path aggregationsDir = Paths.get("test/aggregations/");
		NioEventloop eventloop = new NioEventloop();

		for (int i = 0; i < iterations + 1; ++i) {
			if (i == iterations - 1) {
				Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
				root.setLevel(Level.INFO);
			}
			if (i == iterations) {
				classLoader = new DefiningClassLoader();
			}
			logger.info("Benchmark started.");

			logger.info("Initialization started.");
			deleteRecursivelyQuietly(aggregationsDir);
			CubeStructure structure = getStructure(classLoader);
			AggregationStorage aggregationStorage = new LocalFsAggregationStorage(eventloop, executor, structure, aggregationsDir);
			Cube cube = newCube(eventloop, classLoader, new LogToCubeMetadataStorageStub(), aggregationStorage, structure);
			cube.addAggregation(new Aggregation("pub", asList("pub"), asList("pubRequests")));
			cube.addAggregation(new Aggregation("adv", asList("adv"), asList("advRequests")));

			Path dir = Paths.get("test/logs/");
			deleteRecursivelyQuietly(dir);
			LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, dir);
			SerializerFactory bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory(classLoader, true, true);
			SerializerScanner registry = SerializerScanner.defaultScanner();
			SerializerGen serializerGen = registry.serializer(TypeToken.of(TestPubRequest.class));
			BufferSerializer<TestPubRequest> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

			LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

			LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
					"testlog", asList("partitionA"), new LogToCubeMetadataStorageStub());
			logger.info("Initialization finished.");

			int numberOfTestRequests = 100_000;
			logger.info("Started generating {} random requests.", numberOfTestRequests);
			List<TestPubRequest> pubRequests = generatePubRequests(numberOfTestRequests);
			long advRequests = countAdvRequests(pubRequests);
			logger.info("Finished generating random requests. Adv requests generated: {}", advRequests);

			logger.info("Started streaming to LogManager.");
			new StreamProducers.OfIterator<>(eventloop, pubRequests.iterator())
					.streamTo(logManager.consumer("partitionA"));

			eventloop.run();
			logger.info("Finished streaming to LogManager.");

			CompletionCallbackObserver cb = new CompletionCallbackObserver();
			logger.info("Started processing log.");
			logToCubeRunner.processLog(cb);

			eventloop.run();
			logger.info("Finished processing log.");
			cb.check();

			consumerToList = StreamConsumers.toList(eventloop);
			cube.query(0, TestAdvResult.class, new CubeQuery().dimension("adv").measure("advRequests")
					.between("adv", 2500, 7500).orderDesc("advRequests"))
					.streamTo(consumerToList);
			logger.info("Started executing query.");
			eventloop.run();
			logger.info("Finished executing query.");

			logger.info("Benchmark finished.");
		}

		executor.shutdownNow();
	}

	public static List<TestPubRequest> generatePubRequests(int numberOfTestRequests) {
		List<TestPubRequest> pubRequests = new ArrayList<>();

		for (int i = 0; i < numberOfTestRequests; ++i) {
			pubRequests.add(TestPubRequest.randomPubRequest());
		}

		return pubRequests;
	}

	public static long countAdvRequests(List<TestPubRequest> pubRequests) {
		long count = 0;

		for (TestPubRequest pubRequest : pubRequests) {
			count += pubRequest.advRequests.size();
		}

		return count;
	}
}
