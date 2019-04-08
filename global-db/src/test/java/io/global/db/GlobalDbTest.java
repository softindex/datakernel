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

package io.global.db;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.StubHttpClient;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.db.api.DbClient;
import io.global.db.api.DbStorage;
import io.global.db.api.GlobalDbNode;
import io.global.db.http.GlobalDbNodeServlet;
import io.global.db.http.HttpGlobalDbNode;
import io.global.db.stub.RuntimeDbStorageStub;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.db.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
public final class GlobalDbTest {
	private static final RawServerId FIRST_ID = new RawServerId("http://127.0.0.1:1001");
	private static final RawServerId SECOND_ID = new RawServerId("http://127.0.0.1:1002");

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private DiscoveryService discoveryService;

	private KeyPair alice = KeyPair.generate();
	private KeyPair bob = KeyPair.generate();

	private GlobalDbNodeImpl rawSecondClient;

	private GlobalDbDriver firstDriver;
	private GlobalDbAdapter firstAliceAdapter;
	private GlobalDbAdapter secondAliceAdapter;

	private Function<RawServerId, GlobalDbNode> nodeFactory;
	private BiFunction<PubKey, String, DbStorage> storageFactory;

	private DbClient cachingAliceGateway;

	@Before
	public void setUp() throws IOException {
		Path dir = temporaryFolder.newFolder().toPath();
		System.out.println("DIR: " + dir);

		FsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), dir).withRevisions();
		discoveryService = LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), storage.subfolder("discovery"));

		storageFactory = ($1, $2) -> new RuntimeDbStorageStub();

		Map<RawServerId, GlobalDbNode> nodes = new HashMap<>();

		nodeFactory = new Function<RawServerId, GlobalDbNode>() {
			@Override
			public GlobalDbNode apply(RawServerId serverId) {
				GlobalDbNode node = nodes.computeIfAbsent(serverId, id -> GlobalDbNodeImpl.create(id, discoveryService, this, storageFactory));
				MiddlewareServlet servlet = MiddlewareServlet.create()
						.with("/db", GlobalDbNodeServlet.create(node));
				StubHttpClient client = StubHttpClient.of(servlet);
				return HttpGlobalDbNode.create(serverId.getServerIdString(), client);
			}
		};

		GlobalDbNode firstNode = nodeFactory.apply(FIRST_ID);
		GlobalDbNode secondNode = nodeFactory.apply(SECOND_ID);

		rawSecondClient = (GlobalDbNodeImpl) nodes.get(SECOND_ID);

		firstDriver = GlobalDbDriver.create(firstNode);
		GlobalDbDriver secondDriver = GlobalDbDriver.create(secondNode);

		firstAliceAdapter = firstDriver.adapt(alice);
		secondAliceAdapter = secondDriver.adapt(alice);

		GlobalDbNode cachingNode = nodeFactory.apply(new RawServerId("http://127.0.0.1:1003"));
		GlobalDbDriver cachingDriver = GlobalDbDriver.create(cachingNode);
		cachingAliceGateway = cachingDriver.adapt(alice);
	}

	private void announce(KeyPair keys, Set<RawServerId> rawServerIds) {
		await(discoveryService.announce(keys.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), AnnounceData.of(123, rawServerIds), keys.getPrivKey())));
	}

	private static Set<DbItem> createContent() {
		long timestamp = System.currentTimeMillis();
		return set(
				DbItem.of("test 1".getBytes(UTF_8), "value 1".getBytes(UTF_8), timestamp),
				DbItem.of("test 2".getBytes(UTF_8), "value 2".getBytes(UTF_8), timestamp + 10),
				DbItem.of("test 3".getBytes(UTF_8), "value 3".getBytes(UTF_8), timestamp + 25)
		);
	}

	@Test
	public void uploadDownload() {
		Set<DbItem> content = createContent();

		await(ChannelSupplier.ofIterable(content).peek(System.out::println).streamTo(await(firstAliceAdapter.upload("test"))));
		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
	}

	@Test
	public void announcedUpload() {
		announce(alice, set(FIRST_ID, SECOND_ID));

		Set<DbItem> content = createContent();

		// upload to the caching node, it'll cache and also forward to one of the masters
		await(ChannelSupplier.ofIterable(content).streamTo(await(cachingAliceGateway.upload("test"))));

		List<DbItem> firstList = await(await(firstAliceAdapter.download("test")).toList());
		List<DbItem> secondList = await(await(secondAliceAdapter.download("test")).toList());

		// on first or on second but not on both (with default 0-1 setting)
		assertTrue(firstList.isEmpty() ^ secondList.isEmpty());
	}

	@Test
	public void separate() {
		DbClient firstBobAdapter = firstDriver.adapt(bob.getPubKey());

		Set<DbItem> content = createContent();

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
		assertTrue(await(await(firstBobAdapter.download("test")).toList()).isEmpty());
	}

	@Test
	public void downloadFromOther() {

		Set<DbItem> content = createContent();

		RawServerId serverId = new RawServerId("localhost:432");
		GlobalDbNode other = GlobalDbNodeImpl.create(serverId, discoveryService, nodeFactory, storageFactory);
		GlobalDbDriver otherDriver = GlobalDbDriver.create(other);
		DbClient otherClient = otherDriver.adapt(alice.getPubKey());

		announce(alice, set(FIRST_ID, SECOND_ID));
		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(otherClient.download("test")).toCollector(toSet())));
	}

	@Test
	public void uploadToOther() {

		Set<DbItem> content = createContent();

		RawServerId serverId = new RawServerId("localhost:432");
		GlobalDbNode other = GlobalDbNodeImpl.create(serverId, discoveryService, nodeFactory, storageFactory);
		GlobalDbDriver otherDriver = GlobalDbDriver.create(other);
		DbClient otherClient = otherDriver.adapt(alice.getPubKey());

		announce(alice, set(FIRST_ID, SECOND_ID));
		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(otherClient.download("test")).toCollector(toSet())));
	}

	@Test
	public void fetch() {
		Set<DbItem> content = createContent();

		announce(alice, set(FIRST_ID, SECOND_ID));

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		// ping second node so that if would find out that it is master for alice
		await(secondAliceAdapter.list());

		await(rawSecondClient.fetch());

		assertEquals(content, await(await(secondAliceAdapter.download("test")).toCollector(toSet())));
	}

	@Test
	public void push() {
		Set<DbItem> content = createContent();

		await(ChannelSupplier.ofIterable(content).streamTo(await(secondAliceAdapter.upload("test"))));

		announce(alice, set(FIRST_ID));

		await(rawSecondClient.push());

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
	}

	@Test
	public void encryptionAndDriver() {
		SimKey key1 = SimKey.generate();

		Set<DbItem> content = createContent();

		firstAliceAdapter.setCurrentSimKey(key1);

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));

		firstAliceAdapter.setCurrentSimKey(null);

		assertEquals(emptyList(), await(await(firstAliceAdapter.download("test")).toList()));
	}
}
