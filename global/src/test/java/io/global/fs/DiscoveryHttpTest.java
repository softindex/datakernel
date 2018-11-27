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
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.DiscoveryServlet;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
public final class DiscoveryHttpTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		FsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), Executors.newSingleThreadExecutor(), temporaryFolder.newFolder().toPath());
		DiscoveryServlet servlet = new DiscoveryServlet(LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), storage));

		DiscoveryService clientService = HttpDiscoveryService.create(new InetSocketAddress(8080), request -> {
			try {
				return servlet.serve(request);
			} catch (ParseException e) {
				throw new AssertionError(e);
			}
		});

		KeyPair alice = KeyPair.generate();
		KeyPair bob = KeyPair.generate();

		SimKey bobSimKey = SimKey.generate();
		Hash bobSimKeyHash = Hash.sha1(bobSimKey.getBytes());

		InetAddress localhost = InetAddress.getLocalHost();

		AnnounceData testAnnounce = AnnounceData.of(123, set(new RawServerId("127.0.0.1:1234")));

		clientService.announce(alice.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), testAnnounce, alice.getPrivKey()))
				.thenCompose($ -> clientService.announce(bob.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), testAnnounce, bob.getPrivKey())))

				.thenCompose($ -> clientService.find(alice.getPubKey()))
				.whenComplete(assertComplete(data -> assertTrue(data.verify(alice.getPubKey()))))

				.thenCompose($ -> clientService.find(bob.getPubKey()))
				.whenComplete(assertComplete(data -> assertTrue(data.verify(bob.getPubKey()))))

				.thenCompose($ -> clientService.announce(alice.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), AnnounceData.of(90, set()), alice.getPrivKey())))
				.whenComplete(assertFailure(StacklessException.class, "Rejected announce data as outdated"))
				.thenComposeEx(($, e) -> clientService.find(alice.getPubKey()))
				.whenComplete(assertComplete(data -> {
					assertTrue(data.verify(alice.getPubKey()));
					assertEquals(123, data.getValue().getTimestamp());
				}))

				.thenCompose($ -> clientService.shareKey(alice.getPubKey(), SignedData.sign(REGISTRY.get(SharedSimKey.class), SharedSimKey.of(bobSimKey, alice.getPubKey()), bob.getPrivKey())))
				.thenCompose($ -> clientService.getSharedKey(alice.getPubKey(), bobSimKeyHash))
				.whenComplete(assertComplete(signedSharedSimKey -> {
					assertTrue(signedSharedSimKey.verify(bob.getPubKey()));
					assertEquals(bobSimKey, signedSharedSimKey.getValue().decryptSimKey(alice.getPrivKey()));
				}));
	}
}
