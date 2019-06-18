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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.util.BinarySerializers;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static java.util.Collections.singleton;

public final class TestCrdtCluster {
	private static final BinarySerializer<Set<Integer>> INT_SET_SERIALIZER = BinarySerializers.ofSet(INT_SERIALIZER);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Test
	public void testUpload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		CrdtDataSerializer<String, TimestampContainer<Integer>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER));

		List<CrdtServer<String, TimestampContainer<Integer>>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, TimestampContainer<Integer>>> clients = new HashMap<>();
		Map<String, CrdtStorageMap<String, TimestampContainer<Integer>>> remoteStorages = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			CrdtStorageMap<String, TimestampContainer<Integer>> storage = CrdtStorageMap.create(eventloop, TimestampContainer.createCrdtFunction(Integer::max));
			InetSocketAddress address = new InetSocketAddress(5555 + i);
			CrdtServer<String, TimestampContainer<Integer>> server = CrdtServer.create(eventloop, storage, serializer);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(eventloop, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}

		CrdtStorageMap<String, TimestampContainer<Integer>> localStorage = CrdtStorageMap.create(eventloop, TimestampContainer.createCrdtFunction(Integer::max));
		for (int i = 0; i < 25; i++) {
			localStorage.put((char) (i + 97) + "", TimestampContainer.now(i + 1));
		}
		CrdtStorageCluster<String, String, TimestampContainer<Integer>> cluster = CrdtStorageCluster.create(eventloop, clients, TimestampContainer.createCrdtFunction(Integer::max));

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(cluster.upload()))
				.whenComplete(($, e) -> servers.forEach(AbstractServer::close)));
		remoteStorages.forEach((name, storage) -> {
			System.out.println("Data at '" + name + "' storage:");
			storage.iterator().forEachRemaining(System.out::println);
		});
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Test
	public void testDownload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<CrdtServer<String, TimestampContainer<Set<Integer>>>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, TimestampContainer<Set<Integer>>>> clients = new HashMap<>();
		Map<String, CrdtStorageMap<String, TimestampContainer<Set<Integer>>>> remoteStorages = new LinkedHashMap<>();

		CrdtFunction<TimestampContainer<Set<Integer>>> union = TimestampContainer.createCrdtFunction((a, b) -> {
			a.addAll(b);
			return a;
		});
		CrdtDataSerializer<String, TimestampContainer<Set<Integer>>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SET_SERIALIZER));

		for (int i = 0; i < 10; i++) {
			CrdtStorageMap<String, TimestampContainer<Set<Integer>>> storage = CrdtStorageMap.create(eventloop, union);

			storage.put("test_1", TimestampContainer.now(new HashSet<>(singleton(i))));
			storage.put("test_2", TimestampContainer.now(new HashSet<>(singleton(i / 2))));
			storage.put("test_3", TimestampContainer.now(new HashSet<>(singleton(123))));

			InetSocketAddress address = new InetSocketAddress(5555 + i);
			CrdtServer<String, TimestampContainer<Set<Integer>>> server = CrdtServer.create(eventloop, storage, serializer);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(eventloop, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}

		CrdtStorageMap<String, TimestampContainer<Set<Integer>>> localStorage = CrdtStorageMap.create(eventloop, union);
		CrdtStorageCluster<String, String, TimestampContainer<Set<Integer>>> cluster = CrdtStorageCluster.create(eventloop, clients, union);

		await(cluster.download()
				.then(supplier -> supplier
						.streamTo(StreamConsumer.of(localStorage::put))
						.whenComplete(($, e) -> {
							System.out.println("!finish");
							servers.forEach(AbstractServer::close);
						})));

		System.out.println("Data at 'local' storage:");
		localStorage.iterator().forEachRemaining(System.out::println);
		System.out.println();
		remoteStorages.forEach((name, storage) -> {
			System.out.println("Data at '" + name + "' storage:");
			storage.iterator().forEachRemaining(System.out::println);
		});
	}
}
