/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.launchers.crdt;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.config.Config;
import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.CrdtStorageClient;
import io.datakernel.crdt.TimestampContainer;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.promise.Promises;
import io.datakernel.promise.jmx.PromiseStats;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.config.ConfigConverters.ofExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.PUT;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;

@Ignore("manual demos")
public final class CrdtClusterTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	static class TestNodeLauncher extends CrdtNodeLauncher<String, TimestampContainer<Integer>> {
		private final Config config;

		public TestNodeLauncher(Config config) {
			this.config = config;
		}

		@Override
		protected Module getOverrideModule() {
			return new AbstractModule() {
				@Provides
				Config config() {
					return config;
				}
			};
		}

		@Override
		protected CrdtNodeLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
			return new CrdtNodeLogicModule<String, TimestampContainer<Integer>>() {
				@Override
				protected void configure() {
					install(new CrdtHttpModule<String, TimestampContainer<Integer>>() {});
				}

				@Provides
				CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
					return new CrdtDescriptor<>(
							TimestampContainer.createCrdtFunction(Integer::max),
							new CrdtDataSerializer<>(UTF8_SERIALIZER,
									TimestampContainer.createSerializer(INT_SERIALIZER)),
							STRING_CODEC,
							tuple(TimestampContainer::new,
									TimestampContainer::getTimestamp, LONG_CODEC,
									TimestampContainer::getState, INT_CODEC));
				}

				@Provides
				Executor executor(Config config) {
					return config.get(ofExecutor(), "crdt.local.executor");
				}

				@Provides
				FsClient fsClient(Eventloop eventloop, Config config) {
					return LocalFsClient.create(eventloop, config.get(ofPath(), "crdt.local.path"));
				}
			};
		}
	}

	@Test
	public void startFirst() throws Exception {
		new TestNodeLauncher(Config.create()
				.with("crdt.http.listenAddresses", "localhost:7000")
				.with("crdt.server.listenAddresses", "localhost:8000")
				.with("crdt.cluster.server.listenAddresses", "localhost:9000")
				.with("crdt.local.path", "/tmp/TESTS/crdt")
				.with("crdt.cluster.localPartitionId", "first")
				.with("crdt.cluster.replicationCount", "2")
				.with("crdt.cluster.partitions.second", "localhost:8001")
				//				.with("crdt.cluster.partitions.file", "localhost:8002")
		).launch(new String[0]);
	}

	@Test
	public void startSecond() throws Exception {
		new TestNodeLauncher(Config.create()
				.with("crdt.http.listenAddresses", "localhost:7001")
				.with("crdt.server.listenAddresses", "localhost:8001")
				.with("crdt.cluster.server.listenAddresses", "localhost:9001")
				.with("crdt.local.path", "/tmp/TESTS/crdt")
				.with("crdt.cluster.localPartitionId", "second")
				.with("crdt.cluster.replicationCount", "2")
				.with("crdt.cluster.partitions.first", "localhost:8000")
				//				.with("crdt.cluster.partitions.file", "localhost:8002")
		).launch(new String[0]);
	}

	@Test
	public void startFileServer() throws Exception {
		new CrdtFileServerLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtFileServerLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
				return new CrdtFileServerLogicModule<String, TimestampContainer<Integer>>() {};
			}

			@Override
			protected Module getOverrideModule() {
				return new AbstractModule() {
					@Provides
					Config config() {
						return Config.create()
								.with("crdt.localPath", "/tmp/TESTS/fileServer")
								.with("crdt.server.listenAddresses", "localhost:8002");
					}
				};
			}

			@Provides
			CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
				return new CrdtDescriptor<>(
						TimestampContainer.createCrdtFunction(Integer::max),
						new CrdtDataSerializer<>(UTF8_SERIALIZER,
								TimestampContainer.createSerializer(INT_SERIALIZER)),
						STRING_CODEC,
						tuple(TimestampContainer::new,
								TimestampContainer::getTimestamp, LONG_CODEC,
								TimestampContainer::getState, INT_CODEC));
			}
		}.launch(new String[0]);
	}

	@Test
	public void uploadWithHTTP() {
		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());

		PromiseStats uploadStat = PromiseStats.create(Duration.ofSeconds(5));

		StructuredCodec<CrdtData<String, Integer>> codec = tuple(CrdtData::new,
				CrdtData::getKey, STRING_CODEC,
				CrdtData::getState, INT_CODEC);

		Promises.sequence(IntStream.range(0, 1_000_000)
				.mapToObj(i ->
						() -> client.request(HttpRequest.of(PUT, "http://127.0.0.1:7000")
								.withBody(JsonUtils.toJson(codec, new CrdtData<>("value_" + i, i)).getBytes(UTF_8)))
								.toVoid()))
				.whenException(System.err::println)
				.whenComplete(uploadStat.recordStats())
				.whenComplete(assertComplete($ -> System.out.println(uploadStat)));

		// RemoteCrdtClient<String, Integer> client = RemoteCrdtClient.create(eventloop, ADDRESS, CRDT_DATA_SERIALIZER);
		//
		// StreamProducer.ofStream(IntStream.range(0, 1000000)
		// 		.mapToObj(i -> new CrdtData<>("key_" + i, i))).streamTo(client.uploadStream())
		// 		.getEndOfStream()
		// 		.thenRun(() -> System.out.println("finished"));
	}

	@Test
	public void uploadWithStreams() {
		CrdtStorageClient<String, Integer> client = CrdtStorageClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress("localhost", 9000), UTF8_SERIALIZER, INT_SERIALIZER);

		PromiseStats uploadStat = PromiseStats.create(Duration.ofSeconds(5));

		StreamSupplier.ofStream(IntStream.range(0, 1000000)
				.mapToObj(i -> new CrdtData<>("value_" + i, i))).streamTo(StreamConsumer.ofPromise(client.upload()))
				.whenComplete(uploadStat.recordStats())
				.whenComplete(assertComplete($ -> {
					System.out.println(uploadStat);
					System.out.println("finished");
				}));
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Test
	public void downloadStuff() {
		CrdtStorageClient<String, Integer> client = CrdtStorageClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress(9001), UTF8_SERIALIZER, INT_SERIALIZER);

		await(client.download().then(supplier -> supplier.streamTo(StreamConsumer.of(System.out::println))));
	}
}
