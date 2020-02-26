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

import io.datakernel.crdt.CrdtData.CrdtDataSerializer;
import io.datakernel.crdt.local.CrdtClientMap;
import io.datakernel.crdt.remote.CrdtRemoteClient;
import io.datakernel.crdt.remote.CrdtRemoteServer;
import io.datakernel.crdt.primitives.GSet;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AbstractServer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class TestCrdtCluster {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testUpload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		CrdtDataSerializer<String, Integer> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER);

		List<CrdtRemoteServer<String, Integer>> servers = new ArrayList<>();
		Map<String, CrdtClient<String, Integer>> clients = new HashMap<>();
		Map<String, CrdtClientMap<String, Integer>> remoteStorages = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			CrdtClientMap<String, Integer> storage = CrdtClientMap.create(eventloop, Integer::max);
			InetSocketAddress address = new InetSocketAddress(5555 + i);
			CrdtRemoteServer<String, Integer> server = CrdtRemoteServer.create(eventloop, storage, serializer);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtRemoteClient.create(eventloop, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}

		CrdtClientMap<String, Integer> localStorage = CrdtClientMap.create(eventloop, Integer::max);
		for (int i = 0; i < 25; i++) {
			localStorage.put((char) (i + 97) + "", i + 1);
		}
		CrdtCluster<String, String, Integer> cluster = CrdtCluster.create(eventloop, clients, Integer::max);

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(cluster.upload()))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));
		remoteStorages.forEach((name, storage) -> {
			System.out.println("Data at '" + name + "' storage:");
			storage.iterator().forEachRemaining(System.out::println);
		});
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Test
	public void testDownload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<CrdtRemoteServer<String, GSet<Integer>>> servers = new ArrayList<>();
		Map<String, CrdtClient<String, GSet<Integer>>> clients = new HashMap<>();
		Map<String, CrdtClientMap<String, GSet<Integer>>> remoteStorages = new LinkedHashMap<>();

		CrdtDataSerializer<String, GSet<Integer>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, new GSet.Serializer<>(INT_SERIALIZER));

		for (int i = 0; i < 10; i++) {
			CrdtClientMap<String, GSet<Integer>> storage = CrdtClientMap.create(eventloop);

			storage.put("test_1", GSet.of(i));
			storage.put("test_2", GSet.of(i / 2));
			storage.put("test_3", GSet.of(123));

			InetSocketAddress address = new InetSocketAddress(5555 + i);
			CrdtRemoteServer<String, GSet<Integer>> server = CrdtRemoteServer.create(eventloop, storage, serializer);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtRemoteClient.create(eventloop, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}

		CrdtClientMap<String, GSet<Integer>> localStorage = CrdtClientMap.create(eventloop);
		CrdtCluster<String, String, GSet<Integer>> cluster = CrdtCluster.create(eventloop, clients);

		await(cluster.download()
				.then(supplier -> supplier
						.streamTo(StreamConsumer.of(localStorage::put))
						.whenComplete(() -> {
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
