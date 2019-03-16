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

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static java.util.Collections.singleton;

@RunWith(DatakernelRunner.class)
public final class TestCrdtCluster {
	private static final BinarySerializer<Set<Integer>> INT_SET_SERIALIZER = BinarySerializers.ofSet(INT_SERIALIZER);

	@Test
	public void testUpload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<CrdtServer<String, Integer>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, Integer>> clients = new HashMap<>();
		Map<String, CrdtStorageTreeMap<String, Integer>> remoteStorages = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			CrdtStorageTreeMap<String, Integer> storage = CrdtStorageTreeMap.create(eventloop, Math::max);
			InetSocketAddress address = new InetSocketAddress(5555 + i);
			CrdtServer<String, Integer> server = CrdtServer.create(eventloop, storage, UTF8_SERIALIZER, INT_SERIALIZER);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(eventloop, address, UTF8_SERIALIZER, INT_SERIALIZER));
			remoteStorages.put("server_" + i, storage);
		}

		CrdtStorageTreeMap<String, Integer> localStorage = CrdtStorageTreeMap.create(eventloop, Math::max);
		for (int i = 0; i < 25; i++) {
			localStorage.put((char) (i + 97) + "", i + 1);
		}
		CrdtStorageCluster<String, String, Integer> cluster = CrdtStorageCluster.create(eventloop, clients, Math::max);

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(cluster.upload()))
				.acceptEx(($, e) -> servers.forEach(AbstractServer::close)));
		remoteStorages.forEach((name, storage) -> {
			System.out.println("Data at '" + name + "' storage:");
			storage.iterator().forEachRemaining(System.out::println);
		});
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Test
	public void testDownload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<CrdtServer<String, Set<Integer>>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, Set<Integer>>> clients = new HashMap<>();
		Map<String, CrdtStorageTreeMap<String, Set<Integer>>> remoteStorages = new LinkedHashMap<>();

		BinaryOperator<Set<Integer>> union = (a, b) -> {
			a.addAll(b);
			return a;
		};

		for (int i = 0; i < 10; i++) {
			CrdtStorageTreeMap<String, Set<Integer>> storage = CrdtStorageTreeMap.create(eventloop, union);

			storage.put("test_1", new HashSet<>(singleton(i)));
			storage.put("test_2", new HashSet<>(singleton(i / 2)));
			storage.put("test_3", new HashSet<>(singleton(123)));

			InetSocketAddress address = new InetSocketAddress(5555 + i);
			CrdtServer<String, Set<Integer>> server = CrdtServer.create(eventloop, storage, UTF8_SERIALIZER, INT_SET_SERIALIZER);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(eventloop, address, UTF8_SERIALIZER, INT_SET_SERIALIZER));
			remoteStorages.put("server_" + i, storage);
		}

		CrdtStorageTreeMap<String, Set<Integer>> localStorage = CrdtStorageTreeMap.create(eventloop, union);
		CrdtStorageCluster<String, String, Set<Integer>> cluster = CrdtStorageCluster.create(eventloop, clients, union);

		await(cluster.download()
				.then(supplierWithResult -> supplierWithResult
						.streamTo(StreamConsumer.of(localStorage::put))
						.acceptEx(($, e) -> servers.forEach(AbstractServer::close))));

		System.out.println("Data at 'local' storage:");
		localStorage.iterator().forEachRemaining(System.out::println);
		System.out.println();
		remoteStorages.forEach((name, storage) -> {
			System.out.println("Data at '" + name + "' storage:");
			storage.iterator().forEachRemaining(System.out::println);
		});
	}
}
