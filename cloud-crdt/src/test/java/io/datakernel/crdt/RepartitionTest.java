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

import io.datakernel.crdt.local.CrdtStorageMap;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import io.datakernel.util.ref.RefInt;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;

public final class RepartitionTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Test
	public void test() {
		CrdtFunction<TimestampContainer<Integer>> crdtFunction = TimestampContainer.createCrdtFunction(Integer::max);

		Map<String, CrdtStorageMap<String, TimestampContainer<Integer>>> clients = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			CrdtStorageMap<String, TimestampContainer<Integer>> client = CrdtStorageMap.create(Eventloop.getCurrentEventloop(), crdtFunction);
			clients.put("client_" + i, client);
		}
		await(StreamSupplier.ofStream(IntStream.range(1, 100).mapToObj(i -> new CrdtData<>("test" + i, TimestampContainer.now(i))))
				.streamTo(StreamConsumer.ofPromise(clients.get("client_0").upload())));

		CrdtStorageCluster<String, String, TimestampContainer<Integer>> cluster = CrdtStorageCluster.create(Eventloop.getCurrentEventloop(), clients, crdtFunction)
				.withReplicationCount(3);

		await(CrdtRepartitionController.create(cluster, "client_0").repartition());

		clients.forEach((k, v) -> {
			System.out.println(k + ":");
			RefInt count = new RefInt(0);
			v.iterator().forEachRemaining(x -> {
				count.inc();
				System.out.println(x);
			});
			System.out.println("Was " + count.get() + " elements");
		});
	}
}
