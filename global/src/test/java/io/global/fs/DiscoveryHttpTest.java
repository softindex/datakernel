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

package io.global.fs;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.stream.processor.ActiveStagesRule;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.fs.http.DiscoveryServlet;
import io.global.fs.http.HttpDiscoveryService;
import io.global.fs.local.RuntimeDiscoveryService;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.util.CollectionUtils.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DiscoveryHttpTest {

	@Rule
	public ActiveStagesRule activeStagesRule = new ActiveStagesRule();

	@Test
	public void test() throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		DiscoveryService serverService = new RuntimeDiscoveryService();
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, new DiscoveryServlet(serverService)).withListenPort(8080);
		server.listen();

		DiscoveryService clientService = new HttpDiscoveryService(new InetSocketAddress(8080), AsyncHttpClient.create(eventloop));

		KeyPair keys1 = KeyPair.generate();
		KeyPair keys2 = KeyPair.generate();

		InetAddress localhost = InetAddress.getLocalHost();

		clientService.announce(keys1, AnnounceData.of(123, keys1.getPubKey(), set(new RawServerId(new InetSocketAddress(localhost, 123)))))
				.thenCompose($ -> clientService.announce(keys2, AnnounceData.of(124, keys1.getPubKey(), set(new RawServerId(new InetSocketAddress(localhost, 124))))))

				.thenCompose($ -> clientService.findServers(keys1.getPubKey()))
				.whenComplete(assertComplete(data -> assertTrue(data.verify(keys1.getPubKey()))))

				.thenCompose($ -> clientService.findServers(keys2.getPubKey()))
				.whenComplete(assertComplete(data -> assertTrue(data.verify(keys2.getPubKey()))))

				.thenCompose($ -> clientService.announce(keys1, AnnounceData.of(90, keys1.getPubKey(), set())))
				.thenCompose($ -> clientService.findServers(keys1.getPubKey()))
				.whenComplete(assertComplete(data -> {
					assertTrue(data.verify(keys1.getPubKey()));
					assertEquals(123, data.getData().getTimestamp());
				}))

				.whenComplete(($, e) -> server.close());

		eventloop.run();
	}
}
