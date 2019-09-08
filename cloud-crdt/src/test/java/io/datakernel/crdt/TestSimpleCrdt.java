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

import io.datakernel.crdt.local.CrdtStorageMap;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static org.junit.Assert.assertEquals;

public final class TestSimpleCrdt {
	private CrdtStorageMap<String, TimestampContainer<Integer>> remoteStorage;
	private CrdtServer<String, TimestampContainer<Integer>> server;
	private CrdtStorage<String, TimestampContainer<Integer>> client;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setup() throws IOException {
		remoteStorage = CrdtStorageMap.create(getCurrentEventloop(), TimestampContainer.createCrdtFunction(Integer::max));
		remoteStorage.put("mx", TimestampContainer.now(2));
		remoteStorage.put("test", TimestampContainer.now(3));
		remoteStorage.put("test", TimestampContainer.now(5));
		remoteStorage.put("only_remote", TimestampContainer.now(35));
		remoteStorage.put("only_remote", TimestampContainer.now(4));

		server = CrdtServer.create(getCurrentEventloop(), remoteStorage, new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER)));
		server.withListenAddress(new InetSocketAddress(5555)).listen();

		client = CrdtStorageClient.create(getCurrentEventloop(), new InetSocketAddress(5555), new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER)));
	}

	@Test
	public void testUpload() {
		CrdtStorageMap<String, TimestampContainer<Integer>> localStorage = CrdtStorageMap.create(getCurrentEventloop(), TimestampContainer.createCrdtFunction(Integer::max));
		localStorage.put("mx", TimestampContainer.now(22));
		localStorage.put("mx", TimestampContainer.now(2));
		localStorage.put("mx", TimestampContainer.now(23));
		localStorage.put("test", TimestampContainer.now(1));
		localStorage.put("test", TimestampContainer.now(2));
		localStorage.put("test", TimestampContainer.now(4));
		localStorage.put("test", TimestampContainer.now(3));
		localStorage.put("only_local", TimestampContainer.now(47));
		localStorage.put("only_local", TimestampContainer.now(12));

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(client.upload()))
				.whenComplete(($, e) -> server.close()));

		System.out.println("Data at 'remote' storage:");
		remoteStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(23, checkNotNull(remoteStorage.get("mx")).getState().intValue());
		assertEquals(5, checkNotNull(remoteStorage.get("test")).getState().intValue());
		assertEquals(35, checkNotNull(remoteStorage.get("only_remote")).getState().intValue());
		assertEquals(47, checkNotNull(remoteStorage.get("only_local")).getState().intValue());
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Test
	public void testDownload() {
		CrdtStorageMap<String, TimestampContainer<Integer>> localStorage = CrdtStorageMap.create(getCurrentEventloop(), TimestampContainer.createCrdtFunction(Integer::max));

		await(client.download().then(supplierWithResult -> supplierWithResult
				.streamTo(StreamConsumer.of(localStorage::put))
				.whenComplete(($, err) -> server.close())));

		System.out.println("Data fetched from 'remote' storage:");
		localStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(2, checkNotNull(localStorage.get("mx")).getState().intValue());
		assertEquals(5, checkNotNull(localStorage.get("test")).getState().intValue());
		assertEquals(35, checkNotNull(localStorage.get("only_remote")).getState().intValue());
	}
}
