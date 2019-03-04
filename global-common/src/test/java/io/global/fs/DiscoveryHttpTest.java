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
import org.spongycastle.crypto.CryptoException;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.api.DiscoveryService.REJECTED_OUTDATED_ANNOUNCE_DATA;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public final class DiscoveryHttpTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws IOException, CryptoException {
		FsClient storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), temporaryFolder.newFolder().toPath()).withRevisions();
		DiscoveryServlet servlet = DiscoveryServlet.create(LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), storage));

		DiscoveryService clientService = HttpDiscoveryService.create(new InetSocketAddress(8080), servlet::serve);

		KeyPair alice = KeyPair.generate();
		KeyPair bob = KeyPair.generate();

		SimKey bobSimKey = SimKey.generate();
		Hash bobSimKeyHash = Hash.sha1(bobSimKey.getBytes());

		AnnounceData testAnnounce = AnnounceData.of(123, set(new RawServerId("127.0.0.1:1234")));

		await(clientService.announce(alice.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), testAnnounce, alice.getPrivKey())));
		await(clientService.announce(bob.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), testAnnounce, bob.getPrivKey())));

		SignedData<AnnounceData> aliceData = await(clientService.find(alice.getPubKey()));
		assertTrue(aliceData.verify(alice.getPubKey()));

		SignedData<AnnounceData> bobData = await(clientService.find(bob.getPubKey()));
		assertTrue(bobData.verify(bob.getPubKey()));

		StacklessException e = awaitException(clientService.announce(alice.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class),
				AnnounceData.of(90, set()), alice.getPrivKey())));
		assertSame(REJECTED_OUTDATED_ANNOUNCE_DATA, e);

		aliceData = await(clientService.find(alice.getPubKey()));
		assertTrue(aliceData.verify(alice.getPubKey()));
		assertEquals(123, aliceData.getValue().getTimestamp());

		await(clientService.shareKey(alice.getPubKey(), SignedData.sign(REGISTRY.get(SharedSimKey.class),
				SharedSimKey.of(bobSimKey, alice.getPubKey()), bob.getPrivKey())));
		SignedData<SharedSimKey> signedSharedSimKey = await(clientService.getSharedKey(alice.getPubKey(), bobSimKeyHash));
		assertTrue(signedSharedSimKey.verify(bob.getPubKey()));
		assertEquals(bobSimKey, signedSharedSimKey.getValue().decryptSimKey(alice.getPrivKey()));

	}
}
