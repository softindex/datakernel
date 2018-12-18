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

package io.datakernel.crdt;

import io.datakernel.crdt.local.RuntimeCrdtClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class TestSimpleCrdt {
	private RuntimeCrdtClient<String, Integer> remoteStorage;
	private CrdtServer<String, Integer> server;
	private CrdtClient<String, Integer> client;

	@Before
	public void setup() throws IOException {
		remoteStorage = RuntimeCrdtClient.create(getCurrentEventloop(), Integer::max);
		remoteStorage.put("mx", 2);
		remoteStorage.put("test", 3);
		remoteStorage.put("test", 5);
		remoteStorage.put("only_remote", 35);
		remoteStorage.put("only_remote", 4);

		server = CrdtServer.create(getCurrentEventloop(), remoteStorage, UTF8_SERIALIZER, INT_SERIALIZER);
		server.withListenAddress(new InetSocketAddress(8080)).listen();

		client = RemoteCrdtClient.create(getCurrentEventloop(), new InetSocketAddress(8080), UTF8_SERIALIZER, INT_SERIALIZER);
	}

	@Test
	public void testUpload() {
		RuntimeCrdtClient<String, Integer> localStorage = RuntimeCrdtClient.create(getCurrentEventloop(), Math::max);
		localStorage.put("mx", 22);
		localStorage.put("mx", 2);
		localStorage.put("mx", 23);
		localStorage.put("test", 1);
		localStorage.put("test", 2);
		localStorage.put("test", 4);
		localStorage.put("test", 3);
		localStorage.put("only_local", 47);
		localStorage.put("only_local", 12);

		StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(client.upload()))
				// .thenCompose($ -> Promise.ofCallback(cb -> getCurrentEventloop().delay(1000, () -> cb.set(null))))
				.whenComplete(($, e) -> server.close())
				.whenComplete(assertComplete($ -> {
					System.out.println("Data at 'remote' storage:");
					remoteStorage.iterator().forEachRemaining(System.out::println);

					assertEquals(23, checkNotNull(remoteStorage.get("mx")).intValue());
					assertEquals(5, checkNotNull(remoteStorage.get("test")).intValue());
					assertEquals(35, checkNotNull(remoteStorage.get("only_remote")).intValue());
					assertEquals(47, checkNotNull(remoteStorage.get("only_local")).intValue());
				}));
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Test
	public void testDownload() {
		RuntimeCrdtClient<String, Integer> localStorage = RuntimeCrdtClient.create(getCurrentEventloop(), Integer::max);

		client.download().getStream()
				.streamTo(StreamConsumer.of(localStorage::put))
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete($ -> {
					System.out.println("Data fetched from 'remote' storage:");
					localStorage.iterator().forEachRemaining(System.out::println);

					assertEquals(2, checkNotNull(localStorage.get("mx")).intValue());
					assertEquals(5, checkNotNull(localStorage.get("test")).intValue());
					assertEquals(35, checkNotNull(localStorage.get("only_remote")).intValue());
				}));
	}
}
