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
import io.datakernel.stream.processor.ActivePromisesRule;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.fs.http.DiscoveryServlet;
import io.global.fs.http.HttpDiscoveryService;
import io.global.fs.local.RuntimeDiscoveryService;
import org.junit.Rule;
import org.junit.Test;
import org.spongycastle.crypto.CryptoException;

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
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Test
	public void test() throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		DiscoveryServlet servlet = new DiscoveryServlet(new RuntimeDiscoveryService());

		DiscoveryService clientService = HttpDiscoveryService.create(new InetSocketAddress(8080), request -> {
			try {
				return servlet.serve(request);
			} catch (ParseException e) {
				throw new AssertionError(e);
			}
		});

		KeyPair alice = KeyPair.generate();
		RepoID aliceTestFs = RepoID.of(alice, "testFs");

		KeyPair bob = KeyPair.generate();
		SimKey bobSimKey = SimKey.generate();
		RepoID bobTestFs = RepoID.of(bob, "testFs");

		Hash bobSimKeyHash = Hash.of(bobSimKey);

		InetAddress localhost = InetAddress.getLocalHost();

		AnnounceData testAnnounce = AnnounceData.of(123, set(new RawServerId(new InetSocketAddress(localhost, 123))));

		clientService.announceSpecific(aliceTestFs, testAnnounce, alice.getPrivKey())
				.thenCompose($ -> clientService.announceSpecific(bobTestFs, testAnnounce, bob.getPrivKey()))

				.thenCompose($ -> clientService.findSpecific(aliceTestFs))
				.whenComplete(assertComplete(data -> {
					assertTrue(data.isPresent());
					assertTrue(data.get().verify(alice.getPubKey()));
				}))

				.thenCompose($ -> clientService.findSpecific(bobTestFs))
				.whenComplete(assertComplete(data -> {
					assertTrue(data.isPresent());
					assertTrue(data.get().verify(bob.getPubKey()));
				}))

				.thenCompose($ -> clientService.announceSpecific(aliceTestFs, AnnounceData.of(90, set()), alice.getPrivKey()))
				.thenCompose($ -> clientService.findSpecific(aliceTestFs))
				.whenComplete(assertComplete(data -> {
					assertTrue(data.isPresent());
					assertTrue(data.get().verify(alice.getPubKey()));
					assertEquals(123, data.get().getData().getTimestamp());
				}))

				.thenCompose($ -> clientService.shareKey(bob, SharedSimKey.of(alice.getPubKey(), bobSimKey)))
				.thenCompose($ -> clientService.getSharedKey(bob.getPubKey(), alice.getPubKey(), bobSimKeyHash))
				.whenComplete(assertComplete(response -> {
					assertTrue(response.isPresent());
					SignedData<SharedSimKey> signed = response.get();
					assertTrue(signed.verify(bob.getPubKey()));
					SharedSimKey sharedSimKey = signed.getData();
					try {
						System.out.println(sharedSimKey);
						assertEquals(bobSimKey, sharedSimKey.decryptSimKey(alice.getPrivKey()));
					} catch (CryptoException e) {
						throw new AssertionError(e);
					}
				}));

		eventloop.run();
	}
}
