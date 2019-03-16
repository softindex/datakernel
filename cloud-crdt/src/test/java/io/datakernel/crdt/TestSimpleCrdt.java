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

package io.datakernel.crdt;

import io.datakernel.crdt.local.CrdtStorageTreeMap;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class TestSimpleCrdt {
	private CrdtStorageTreeMap<String, Integer> remoteStorage;
	private CrdtServer<String, Integer> server;
	private CrdtStorage<String, Integer> client;

	@Before
	public void setup() throws IOException {
		remoteStorage = CrdtStorageTreeMap.create(getCurrentEventloop(), Integer::max);
		remoteStorage.put("mx", 2);
		remoteStorage.put("test", 3);
		remoteStorage.put("test", 5);
		remoteStorage.put("only_remote", 35);
		remoteStorage.put("only_remote", 4);

		server = CrdtServer.create(getCurrentEventloop(), remoteStorage, UTF8_SERIALIZER, INT_SERIALIZER);
		server.withListenAddress(new InetSocketAddress(5555)).listen();

		client = CrdtStorageClient.create(getCurrentEventloop(), new InetSocketAddress(5555), UTF8_SERIALIZER, INT_SERIALIZER);
	}

	@Test
	public void testUpload() {
		CrdtStorageTreeMap<String, Integer> localStorage = CrdtStorageTreeMap.create(getCurrentEventloop(), Math::max);
		localStorage.put("mx", 22);
		localStorage.put("mx", 2);
		localStorage.put("mx", 23);
		localStorage.put("test", 1);
		localStorage.put("test", 2);
		localStorage.put("test", 4);
		localStorage.put("test", 3);
		localStorage.put("only_local", 47);
		localStorage.put("only_local", 12);

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(client.upload()))
				.acceptEx(($, e) -> server.close()));

		System.out.println("Data at 'remote' storage:");
		remoteStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(23, checkNotNull(remoteStorage.get("mx")).intValue());
		assertEquals(5, checkNotNull(remoteStorage.get("test")).intValue());
		assertEquals(35, checkNotNull(remoteStorage.get("only_remote")).intValue());
		assertEquals(47, checkNotNull(remoteStorage.get("only_local")).intValue());
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Test
	public void testDownload() {
		CrdtStorageTreeMap<String, Integer> localStorage = CrdtStorageTreeMap.create(getCurrentEventloop(), Integer::max);

		await(client.download().then(supplierWithResult -> supplierWithResult
				.streamTo(StreamConsumer.of(localStorage::put))
				.acceptEx(($, err) -> server.close())));

		System.out.println("Data fetched from 'remote' storage:");
		localStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(2, checkNotNull(localStorage.get("mx")).intValue());
		assertEquals(5, checkNotNull(localStorage.get("test")).intValue());
		assertEquals(35, checkNotNull(localStorage.get("only_remote")).intValue());
	}
}
