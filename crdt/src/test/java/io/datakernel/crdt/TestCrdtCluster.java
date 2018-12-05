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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.util.BinarySerializers;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.BinaryOperator;

import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.Collections.singleton;

@RunWith(DatakernelRunner.class)
public final class TestCrdtCluster {
	private static final BinarySerializer<Set<Integer>> INT_SET_SERIALIZER = BinarySerializers.ofSet(INT_SERIALIZER);

	@Test
	public void testUpload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<CrdtServer<String, Integer>> servers = new ArrayList<>();
		Map<String, CrdtClient<String, Integer>> clients = new HashMap<>();
		Map<String, RuntimeCrdtClient<String, Integer>> remoteStorages = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			RuntimeCrdtClient<String, Integer> storage = RuntimeCrdtClient.create(eventloop, Math::max);
			InetSocketAddress address = new InetSocketAddress(8080 + i);
			CrdtServer<String, Integer> server = CrdtServer.create(eventloop, storage, UTF8_SERIALIZER, INT_SERIALIZER);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, RemoteCrdtClient.create(eventloop, address, UTF8_SERIALIZER, INT_SERIALIZER));
			remoteStorages.put("server_" + i, storage);
		}

		RuntimeCrdtClient<String, Integer> localStorage = RuntimeCrdtClient.create(eventloop, Math::max);
		for (int i = 0; i < 25; i++) {
			localStorage.put((char) (i + 97) + "", i + 1);
		}
		CrdtClusterClient<String, String, Integer> cluster = CrdtClusterClient.create(eventloop, clients, Math::max);

		StreamSupplier.ofIterator(localStorage.iterator()).streamTo(cluster.uploader())
				.whenComplete(($, err) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertComplete($ ->
						remoteStorages.forEach((name, storage) -> {
							System.out.println("Data at '" + name + "' storage:");
							storage.iterator().forEachRemaining(System.out::println);
						})));
	}

	@Test
	public void testDownload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<CrdtServer<String, Set<Integer>>> servers = new ArrayList<>();
		Map<String, CrdtClient<String, Set<Integer>>> clients = new HashMap<>();
		Map<String, RuntimeCrdtClient<String, Set<Integer>>> remoteStorages = new LinkedHashMap<>();

		BinaryOperator<Set<Integer>> union = (a, b) -> {
			a.addAll(b);
			return a;
		};

		for (int i = 0; i < 10; i++) {
			RuntimeCrdtClient<String, Set<Integer>> storage = RuntimeCrdtClient.create(eventloop, union);

			storage.put("test_1", new HashSet<>(singleton(i)));
			storage.put("test_2", new HashSet<>(singleton(i / 2)));
			storage.put("test_3", new HashSet<>(singleton(123)));

			InetSocketAddress address = new InetSocketAddress(8080 + i);
			CrdtServer<String, Set<Integer>> server = CrdtServer.create(eventloop, storage, UTF8_SERIALIZER, INT_SET_SERIALIZER);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, RemoteCrdtClient.create(eventloop, address, UTF8_SERIALIZER, INT_SET_SERIALIZER));
			remoteStorages.put("server_" + i, storage);
		}

		RuntimeCrdtClient<String, Set<Integer>> localStorage = RuntimeCrdtClient.create(eventloop, union);
		CrdtClusterClient<String, String, Set<Integer>> cluster = CrdtClusterClient.create(eventloop, clients, union);

		cluster.download().getStream()
				.streamTo(StreamConsumer.of(localStorage::put))
				.whenComplete(($, err) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertComplete($ -> {
					System.out.println("Data at 'local' storage:");
					localStorage.iterator().forEachRemaining(System.out::println);
					System.out.println();
					remoteStorages.forEach((name, storage) -> {
						System.out.println("Data at '" + name + "' storage:");
						storage.iterator().forEachRemaining(System.out::println);
					});
				}));
	}
}
