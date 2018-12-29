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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;

@RunWith(DatakernelRunner.class)
public final class RepartitionTest {

	@Test
	public void test() {
		Map<String, RuntimeCrdtClient<String, Integer>> clients = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			RuntimeCrdtClient<String, Integer> client = RuntimeCrdtClient.create(Eventloop.getCurrentEventloop(), Integer::max);
			clients.put("client_" + i, client);
		}
		await(StreamSupplier.ofStream(IntStream.range(1, 100).mapToObj(i -> new CrdtData<>("test" + i, i)))
				.streamTo(StreamConsumer.ofPromise(((CrdtClient<String, Integer>) clients.get("client_0")).upload())));

		CrdtClusterClient<String, String, Integer> cluster = CrdtClusterClient.create(Eventloop.getCurrentEventloop(), clients, Integer::max)
				.withReplicationCount(3);

		await(CrdtRepartitionController.create(cluster, "client_0").repartition());


		clients.forEach((k, v) -> {
			System.out.println(k + ":");
			int[] i = {0};
			v.iterator().forEachRemaining(x -> {
				i[0]++;
				System.out.println(x);
			});
			System.out.println("Was " + i[0] + " elements");
		});
	}
}
