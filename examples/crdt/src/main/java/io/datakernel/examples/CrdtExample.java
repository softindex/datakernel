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

package io.datakernel.examples;

import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.CrdtStorage;
import io.datakernel.crdt.CrdtStorageCluster;
import io.datakernel.crdt.local.CrdtStorageFs;
import io.datakernel.crdt.primitives.LWWSet;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;

public final class CrdtExample {
	static {
		LoggerConfigurer.enableLogging();
	}
	private static final CrdtDataSerializer<String, LWWSet<String>> SERIALIZER =
			new CrdtDataSerializer<>(UTF8_SERIALIZER, new LWWSet.Serializer<>(UTF8_SERIALIZER));

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create()
				.withCurrentThread();

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Map<Integer, CrdtStorage<String, LWWSet<String>>> clients = new HashMap<>();

		for (int i = 0; i < 8; i++) {
			clients.put(i, createClient(eventloop, i));
		}

		CrdtStorageFs<String, LWWSet<String>> one = createClient(eventloop, 8);
		CrdtStorageFs<String, LWWSet<String>> two = createClient(eventloop, 9);

		CrdtStorageCluster<Integer, String, LWWSet<String>> cluster = CrdtStorageCluster.create(eventloop, clients)
				.withPartition(8, one)
				.withPartition(9, two)
				.withReplicationCount(5);

		// * first replica:
		//   first = [#1, #2, #3, #4]
		//   second = ["#3", "#4", "#5", "#6"]
		//
		// * second replica:
		//   first = [#3, #4, #5, #6]
		//   second = [#2, #4, <removed> #5, <removed> #6]
		//
		// * expected result from the cluster:
		//   first = [#1, #2, #3, #4, #5, #6]
		//   second = [#2, #3, #4]

		StreamSupplier.of(new CrdtData<>("first", LWWSet.of("#1", "#2", "#3", "#4")), new CrdtData<>("second", LWWSet.of("#3", "#4", "#5", "#6")))
				.streamTo(StreamConsumer.ofPromise(one.upload()))
				.then($ -> {
					LWWSet<String> second = LWWSet.of("#2", "#4");
					second.remove("#5");
					second.remove("#6");
					return StreamSupplier.of(new CrdtData<>("first", LWWSet.of("#3", "#4", "#5", "#6")), new CrdtData<>("second", second))
							.streamTo(StreamConsumer.ofPromise(two.upload()));
				})
				.then($ -> cluster.download())
				.then(StreamSupplier::toList)
				.whenComplete((list, e) -> {
					executor.shutdown();
					if (e != null) {
						throw new AssertionError(e);
					}
					System.out.println(list);
				});

		eventloop.run();
	}

	private static CrdtStorageFs<String, LWWSet<String>> createClient(Eventloop eventloop, int n) {
		FsClient storage = LocalFsClient.create(eventloop, Paths.get("/tmp/TESTS/crdt_" + n));
		return CrdtStorageFs.create(eventloop, storage, SERIALIZER);
	}
}




