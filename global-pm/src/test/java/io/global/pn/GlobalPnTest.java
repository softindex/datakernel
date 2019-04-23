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

package io.global.pn;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.pn.api.GlobalPmNode;
import io.global.pn.api.Message;
import io.global.pn.api.MessageStorage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

@RunWith(DatakernelRunner.class)
public final class GlobalPnTest {

	private static final RawServerId FIRST_ID = new RawServerId("http://127.0.0.1:1001");

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private DiscoveryService discovery;

	private KeyPair alice = KeyPair.generate();
	private KeyPair bob = KeyPair.generate();

	private static Function<RawServerId, GlobalPmNode> clientFactory;
	private FsClient storage;
	private Path dir;

	@Before
	public void setUp() throws IOException {
		dir = temporaryFolder.newFolder().toPath();
		storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), dir).withRevisions();

		RawServerId self = new RawServerId("test");
		discovery = LocalDiscoveryService.create(getCurrentEventloop(), storage.subfolder("discovery"));

		MessageStorage messageStorage = new FsMessageStorage(storage.subfolder("messages"));
		Map<RawServerId, GlobalPmNode> nodes = new HashMap<>();

		clientFactory = new Function<RawServerId, GlobalPmNode>() {
			@Override
			public GlobalPmNode apply(RawServerId serverId) {
				GlobalPmNode node = nodes.computeIfAbsent(serverId, id -> new LocalGlobalPmNode(serverId, discovery, this, messageStorage));
				// StubHttpClient client = StubHttpClient.of(GlobalPnNodeServlet.create(node));
				// return HttpGlobalPnNode.create(serverId.getServerIdString(), client);
				return node;
			}
		};
	}

	@Test
	public void test() {
		GlobalPmNode node = clientFactory.apply(FIRST_ID);
		GlobalPmDriver<String> driver = new GlobalPmDriver<>(node, STRING_CODEC);

		for (int i = 0; i < 5; i++) {
			KeyPair keys = KeyPair.generate();
			await(driver.send(keys.getPrivKey(), bob.getPubKey(), Message.now(keys.getPubKey(), "hello! #" + i)));
		}

		await(driver.send(alice.getPrivKey(), bob.getPubKey(), Message.now(alice.getPubKey(), "hello!")));

		System.out.println(await(storage.list("**")));

		await(ChannelSupplier.ofPromise(driver.multipoll(bob))
				.streamTo(ChannelConsumer.of(message -> {
					System.out.println(message);
					return driver.drop(bob, message.getId());
				})));
	}
}
