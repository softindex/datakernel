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

package io.datakernel.crdt;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.crdt.local.FsCrdtClient;
import io.datakernel.crdt.local.RocksDBCrdtClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BinaryOperator;

import static io.datakernel.serializer.asm.BufferSerializers.INT_SERIALIZER;
import static io.datakernel.serializer.asm.BufferSerializers.JAVA_UTF8_SERIALIZER;
import static io.datakernel.test.TestUtils.assertComplete;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(DatakernelRunner.DatakernelRunnerFactory.class)
public class CrdtClientAPITest {
	private static final CrdtDataSerializer<String, Integer> serializer = new CrdtDataSerializer<>(JAVA_UTF8_SERIALIZER, INT_SERIALIZER);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Parameter()
	public String testName;

	@Parameter(1)
	public ICrdtClientFactory<String, Integer> clientFactory;

	private CrdtClient<String, Integer> client;

	@Before
	public void setup() throws Exception {
		// Path folder = temporaryFolder.newFolder().toPath();
		Path folder = Paths.get("/tmp/TESTS");
		Runtime.getRuntime().exec("rm -r /tmp/TESTS").waitFor();
		Files.createDirectories(folder);
		client = clientFactory.create(Executors.newSingleThreadExecutor(), folder, Math::max);
	}

	@After
	public void tearDown() {
		if (client instanceof RocksDBCrdtClient) {
			((RocksDBCrdtClient) client).getDb().close();
		}
	}

	@FunctionalInterface
	private interface ICrdtClientFactory<K extends Comparable<K>, S> {

		CrdtClient<K, S> create(ExecutorService executor, Path testFolder, BinaryOperator<S> combiner) throws Exception;
	}

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{
						"FsCrdtClient",
						(ICrdtClientFactory<String, Integer>) (executor, testFolder, combiner) -> {
							Eventloop eventloop = Eventloop.getCurrentEventloop();
							return FsCrdtClient.create(eventloop, LocalFsClient.create(eventloop, executor, testFolder), combiner, serializer);
						}
				},
				new Object[]{
						"RocksDBCrdtClient",
						(ICrdtClientFactory<String, Integer>) (executor, testFolder, combiner) -> {
							Options options = new Options()
									.setCreateIfMissing(true)
									.setComparator(new RocksDBCrdtClient.KeyComparator<>(JAVA_UTF8_SERIALIZER));
							RocksDB rocksdb = RocksDB.open(options, testFolder.resolve("rocksdb").normalize().toString());
							return RocksDBCrdtClient.create(Eventloop.getCurrentEventloop(), executor, rocksdb, combiner, serializer);
						}
				}
		);
	}

	@Test
	public void testUploadDownload() {
		List<CrdtData<String, Integer>> expected = Arrays.asList(
				new CrdtData<>("test_0", 0),
				new CrdtData<>("test_1", 345),
				new CrdtData<>("test_2", 44),
				new CrdtData<>("test_3", 74),
				new CrdtData<>("test_4", -28)
		);

		Promise.complete()
				.thenCompose($ -> StreamSupplier.of(
						new CrdtData<>("test_1", 344),
						new CrdtData<>("test_2", 24),
						new CrdtData<>("test_3", -8)).streamTo(client.uploader()))
				.thenCompose($ -> StreamSupplier.of(
						new CrdtData<>("test_2", 44),
						new CrdtData<>("test_3", 74),
						new CrdtData<>("test_4", -28)).streamTo(client.uploader()))
				.thenCompose($ -> StreamSupplier.of(
						new CrdtData<>("test_0", 0),
						new CrdtData<>("test_1", 345),
						new CrdtData<>("test_2", -28)).streamTo(client.uploader()))
				.thenCompose($ -> client.download().getStream().toList())
				.whenComplete(assertComplete(list -> {
					// list.sort(Comparator.naturalOrder()); // data should be already sorted by merge-reduce, so check for the order too
					System.out.println(list);
					assertEquals(expected, list);
				}));
	}

	@Test
	public void testDelete() {
		List<CrdtData<String, Integer>> expected = Arrays.asList(
				new CrdtData<>("test_1", 2),
				new CrdtData<>("test_3", 4)
		);
		Promises.all(
				StreamSupplier.of(
						new CrdtData<>("test_1", 1),
						new CrdtData<>("test_2", 2),
						new CrdtData<>("test_3", 4)
				).streamTo(client.uploader()),
				StreamSupplier.of(
						new CrdtData<>("test_1", 2),
						new CrdtData<>("test_2", 3),
						new CrdtData<>("test_3", 2)
				).streamTo(client.uploader())
		)
				.thenCompose($ -> StreamSupplier.of("test_2").streamTo(client.remover()))
				.thenCompose($ -> client.download().getStreamPromise())
				.thenCompose(StreamSupplier::toList)
				.whenComplete(assertComplete(list -> {
					System.out.println(list);
					assertEquals(expected, list);
				}));
	}
}
