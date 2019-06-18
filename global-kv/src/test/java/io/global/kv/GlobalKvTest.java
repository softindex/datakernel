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

package io.global.kv;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StubHttpClient;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvClient;
import io.global.kv.api.KvItem;
import io.global.kv.api.KvStorage;
import io.global.kv.http.GlobalKvNodeServlet;
import io.global.kv.http.HttpGlobalKvNode;
import io.global.kv.stub.RuntimeKvStorageStub;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.kv.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class GlobalKvTest {
	private static final RawServerId FIRST_ID = new RawServerId("http://127.0.0.1:1001");
	private static final RawServerId SECOND_ID = new RawServerId("http://127.0.0.1:1002");

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	private DiscoveryService discoveryService;

	private KeyPair alice = KeyPair.generate();
	private KeyPair bob = KeyPair.generate();

	private LocalGlobalKvNode rawSecondClient;

	private GlobalKvDriver<String, String> firstDriver;
	private GlobalKvAdapter<String, String> firstAliceAdapter;
	private GlobalKvAdapter<String, String> secondAliceAdapter;

	private Function<RawServerId, GlobalKvNode> nodeFactory;
	private BiFunction<PubKey, String, KvStorage> storageFactory;

	private KvClient<String, String> cachingAliceGateway;

	@Before
	public void setUp() throws IOException {
		Path dir = temporaryFolder.newFolder().toPath();
		System.out.println("DIR: " + dir);

		FsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), dir).withRevisions();
		discoveryService = LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), storage.subfolder("discovery"));

		storageFactory = ($1, $2) -> new RuntimeKvStorageStub();

		Map<RawServerId, GlobalKvNode> nodes = new HashMap<>();

		nodeFactory = new Function<RawServerId, GlobalKvNode>() {
			@Override
			public GlobalKvNode apply(RawServerId serverId) {
				GlobalKvNode node = nodes.computeIfAbsent(serverId, id -> LocalGlobalKvNode.create(id, discoveryService, this, storageFactory));
				RoutingServlet servlet = RoutingServlet.create()
						.with("/kv/*", GlobalKvNodeServlet.create(node));
				StubHttpClient client = StubHttpClient.of(servlet);
				return HttpGlobalKvNode.create(serverId.getServerIdString(), client);
			}
		};

		GlobalKvNode firstNode = nodeFactory.apply(FIRST_ID);
		GlobalKvNode secondNode = nodeFactory.apply(SECOND_ID);

		rawSecondClient = (LocalGlobalKvNode) nodes.get(SECOND_ID);

		firstDriver = GlobalKvDriver.create(firstNode, STRING_CODEC, STRING_CODEC);
		GlobalKvDriver<String, String> secondDriver = GlobalKvDriver.create(secondNode, STRING_CODEC, STRING_CODEC);

		firstAliceAdapter = firstDriver.adapt(alice);
		secondAliceAdapter = secondDriver.adapt(alice);

		GlobalKvNode cachingNode = nodeFactory.apply(new RawServerId("http://127.0.0.1:1003"));
		GlobalKvDriver<String, String> cachingDriver = GlobalKvDriver.create(cachingNode, STRING_CODEC, STRING_CODEC);
		cachingAliceGateway = cachingDriver.adapt(alice);
	}

	private void announce(KeyPair keys, Set<RawServerId> rawServerIds) {
		await(discoveryService.announce(keys.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), AnnounceData.of(123, rawServerIds), keys.getPrivKey())));
	}

	private static Set<KvItem<String, String>> createContent() {
		long timestamp = System.currentTimeMillis();
		return set(
				new KvItem<>(timestamp, "test 1", "value 1"),
				new KvItem<>(timestamp + 10, "test 2", "value 2"),
				new KvItem<>(timestamp + 25, "test 3", "value 3"));
	}

	@Test
	public void uploadDownload() {
		Set<KvItem<String, String>> content = createContent();

		await(ChannelSupplier.ofIterable(content).peek(System.out::println).streamTo(await(firstAliceAdapter.upload("test"))));
		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
	}

	@Test
	public void announcedUpload() {
		announce(alice, set(FIRST_ID, SECOND_ID));

		Set<KvItem<String, String>> content = createContent();

		// upload to the caching node, it'll cache and also forward to one of the masters
		await(ChannelSupplier.ofIterable(content).streamTo(await(cachingAliceGateway.upload("test"))));

		List<KvItem<String, String>> firstList = await(await(firstAliceAdapter.download("test")).toList());
		List<KvItem<String, String>> secondList = await(await(secondAliceAdapter.download("test")).toList());

		// on first or on second but not on both (with default 0-1 setting)
		assertTrue(firstList.isEmpty() ^ secondList.isEmpty());
	}

	@Test
	public void separate() {
		KvClient<String, String> firstBobAdapter = firstDriver.adapt(bob.getPubKey());

		Set<KvItem<String, String>> content = createContent();

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
		assertTrue(await(await(firstBobAdapter.download("test")).toList()).isEmpty());
	}

	@Test
	public void downloadFromOther() {

		Set<KvItem<String, String>> content = createContent();

		RawServerId serverId = new RawServerId("localhost:432");
		GlobalKvNode other = LocalGlobalKvNode.create(serverId, discoveryService, nodeFactory, storageFactory);
		GlobalKvDriver<String, String> otherDriver = GlobalKvDriver.create(other, STRING_CODEC, STRING_CODEC);
		KvClient<String, String> otherClient = otherDriver.adapt(alice.getPubKey());

		announce(alice, set(FIRST_ID, SECOND_ID));
		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(otherClient.download("test")).toCollector(toSet())));
	}

	@Test
	public void uploadToOther() {

		Set<KvItem<String, String>> content = createContent();

		RawServerId serverId = new RawServerId("localhost:432");
		GlobalKvNode other = LocalGlobalKvNode.create(serverId, discoveryService, nodeFactory, storageFactory);
		GlobalKvDriver<String, String> otherDriver = GlobalKvDriver.create(other, STRING_CODEC, STRING_CODEC);
		KvClient<String, String> otherClient = otherDriver.adapt(alice.getPubKey());

		announce(alice, set(FIRST_ID, SECOND_ID));
		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(otherClient.download("test")).toCollector(toSet())));
	}

	@Test
	public void fetch() {
		Set<KvItem<String, String>> content = createContent();

		announce(alice, set(FIRST_ID, SECOND_ID));

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		// ping second node so that if would find out that it is master for alice
		await(secondAliceAdapter.list());

		await(rawSecondClient.fetch());

		assertEquals(content, await(await(secondAliceAdapter.download("test")).toCollector(toSet())));
	}

	@Test
	public void push() {
		Set<KvItem<String, String>> content = createContent();

		await(ChannelSupplier.ofIterable(content).streamTo(await(secondAliceAdapter.upload("test"))));

		announce(alice, set(FIRST_ID));

		await(rawSecondClient.push());

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
	}

	@Test
	public void encryptionAndDriver() {
		SimKey key1 = SimKey.generate();

		Set<KvItem<String, String>> content = createContent();

		firstAliceAdapter.setCurrentSimKey(key1);

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));

		firstAliceAdapter.setCurrentSimKey(null);

		assertEquals(emptyList(), await(await(firstAliceAdapter.download("test")).toList()));
	}
}
