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

package io.global.pm;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.StubHttpClient;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.Message;
import io.global.pm.api.MessageStorage;
import io.global.pm.http.GlobalPmNodeServlet;
import io.global.pm.http.HttpGlobalPmNode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class GlobalPmTest {

	private static final RawServerId FIRST_ID = new RawServerId("http://127.0.0.1:1001");
	private static final String MAIL_BOX = "test mail box";

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

		MessageStorage messageStorage = FsMessageStorage.create(storage.subfolder("messages"));
		Map<RawServerId, GlobalPmNode> nodes = new HashMap<>();

		clientFactory = new Function<RawServerId, GlobalPmNode>() {
			@Override
			public GlobalPmNode apply(RawServerId serverId) {
				GlobalPmNode node = nodes.computeIfAbsent(serverId, id -> GlobalPmNodeImpl.create(serverId, discovery, this, messageStorage));

				AsyncServlet servlet = GlobalPmNodeServlet.create(node);
				StubHttpClient client = StubHttpClient.of(MiddlewareServlet.create().with("/pm", servlet));
				return HttpGlobalPmNode.create(serverId.getServerIdString(), client);
//				return node;
			}
		};
	}

	@Test
	public void test() {
		GlobalPmNode node = clientFactory.apply(FIRST_ID);
		GlobalPmDriver<String> driver = new GlobalPmDriver<>(node, STRING_CODEC);

		Set<Message<String>> sent = new HashSet<>();

		for (int i = 0; i < 5; i++) {
			KeyPair keys = KeyPair.generate();
			Message<String> message = Message.now(keys.getPubKey(), "hello! #" + i);
			sent.add(message);
			await(driver.send(keys.getPrivKey(), bob.getPubKey(), MAIL_BOX, message));
		}

		Message<String> msg = Message.now(alice.getPubKey(), "hello!");
		sent.add(msg);
		await(driver.send(alice.getPrivKey(), bob.getPubKey(), MAIL_BOX, msg));

		Set<Message<String>> received = new HashSet<>();

		await(ChannelSupplier.ofPromise(driver.multipoll(bob, MAIL_BOX))
				.streamTo(ChannelConsumer.of(message -> {
					received.add(message);
					return driver.drop(bob, MAIL_BOX, message.getId());
				})));

		assertEquals(sent, received);
	}
}
