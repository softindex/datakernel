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
import io.datakernel.http.StubHttpClient;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.ByteBufRule.IgnoreLeaks;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.db.api.DbClient;
import io.global.db.api.DbStorage;
import io.global.db.api.GlobalDbNode;
import io.global.db.api.TableID;
import io.global.db.http.GlobalDbNodeServlet;
import io.global.db.http.HttpGlobalDbNode;
import io.global.db.stub.RuntimeDbStorageStub;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.util.CollectionUtils.list;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.db.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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

	private LocalGlobalDbNode rawSecondClient;

	private GlobalDbDriver firstDriver;
	private DbClient firstAliceAdapter;
	private DbClient secondAliceAdapter;

	private NodeFactory<GlobalDbNode> nodeFactory;
	private Function<TableID, DbStorage> storageFactory;

	private DbClient cachingAliceGateway;

	@Before
	public void setUp() throws IOException {
		Path dir = temporaryFolder.newFolder().toPath();
		System.out.println("DIR: " + dir);

		FsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), dir).withRevisions();
		discoveryService = LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), storage.subfolder("discovery"));

		storageFactory = $ -> new RuntimeDbStorageStub();

		Map<RawServerId, GlobalDbNode> nodes = new HashMap<>();

		nodeFactory = new NodeFactory<GlobalDbNode>() {
			@Override
			public GlobalDbNode create(RawServerId serverId) {
				GlobalDbNode node = nodes.computeIfAbsent(serverId, id -> LocalGlobalDbNode.create(id, discoveryService, this, storageFactory));
				StubHttpClient client = StubHttpClient.of(GlobalDbNodeServlet.create(node));
				return HttpGlobalDbNode.create(serverId.getServerIdString(), client);
			}
		};

		GlobalDbNode firstNode = nodeFactory.create(FIRST_ID);
		GlobalDbNode secondNode = nodeFactory.create(SECOND_ID);

		rawSecondClient = (LocalGlobalDbNode) nodes.get(SECOND_ID);

		firstDriver = GlobalDbDriver.create(firstNode, discoveryService, list(alice, bob));
		GlobalDbDriver secondDriver = GlobalDbDriver.create(secondNode, discoveryService, list(alice, bob));

		firstAliceAdapter = firstDriver.gatewayFor(alice.getPubKey());
		secondAliceAdapter = secondDriver.gatewayFor(alice.getPubKey());

		GlobalDbNode cachingNode = nodeFactory.create(new RawServerId("http://127.0.0.1:1003"));
		GlobalDbDriver cachingDriver = GlobalDbDriver.create(cachingNode, discoveryService, asList(alice, bob));
		cachingAliceGateway = cachingDriver.gatewayFor(alice.getPubKey());
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

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));
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
		DbClient firstBobAdapter = firstDriver.gatewayFor(bob.getPubKey());

		Set<DbItem> content = createContent();

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
		assertTrue(await(await(firstBobAdapter.download("test")).toList()).isEmpty());
	}

	@Test
	public void downloadFromOther() {

		Set<DbItem> content = createContent();

		RawServerId serverId = new RawServerId("localhost:432");
		GlobalDbNode other = LocalGlobalDbNode.create(serverId, discoveryService, nodeFactory, storageFactory);
		GlobalDbDriver otherDriver = GlobalDbDriver.create(other, discoveryService, singletonList(alice));
		DbClient otherClient = otherDriver.gatewayFor(alice.getPubKey());

		announce(alice, set(FIRST_ID, SECOND_ID));
		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(otherClient.download("test")).toCollector(toSet())));
	}

	@Test
	public void uploadToOther() {

		Set<DbItem> content = createContent();

		RawServerId serverId = new RawServerId("localhost:432");
		GlobalDbNode other = LocalGlobalDbNode.create(serverId, discoveryService, nodeFactory, storageFactory);
		GlobalDbDriver otherDriver = GlobalDbDriver.create(other, discoveryService, singletonList(alice));
		DbClient otherClient = otherDriver.gatewayFor(alice.getPubKey());

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
	@Ignore // TODO anton: this is broken completely, fix the Global DB
	@IgnoreLeaks("TODO") // TODO anton: fix this
	public void encryptionAndDriver() {
		SimKey key1 = SimKey.generate();
		SimKey key2 = SimKey.generate();

		PrivateKeyStorage pks = firstDriver.getPrivateKeyStorage();
		pks.changeCurrentSimKey(key1);

		Set<DbItem> content = createContent();

		await(ChannelSupplier.ofIterable(content).streamTo(await(firstAliceAdapter.upload("test"))));

		assertEquals(content, await(await(firstAliceAdapter.download("test")).toCollector(toSet())));

		pks.forget(Hash.sha1(key1.getBytes()));
		pks.changeCurrentSimKey(key2);

		assertEquals(emptyList(), await(await(firstAliceAdapter.download("test")).toList()));

		await(discoveryService.shareKey(alice.getPubKey(),
				SignedData.sign(REGISTRY.get(SharedSimKey.class), SharedSimKey.of(key1, alice.getPubKey()), alice.getPrivKey())));

		assertEquals(new HashSet<>(content), await(await(firstAliceAdapter.download("test")).toCollector(toSet())));
	}
}
