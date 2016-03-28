/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.datakernel.rpc.client.sender.helper.RpcSenderStub;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.hash.ShardingFunction;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.rpc.client.sender.RpcStrategies.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RpcStrategiesTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final int PORT_4 = 10004;
	private static final int PORT_5 = 10005;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);
	private static final InetSocketAddress ADDRESS_4 = new InetSocketAddress(HOST, PORT_4);
	private static final InetSocketAddress ADDRESS_5 = new InetSocketAddress(HOST, PORT_5);

	@Test
	public void testCombination1() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		int iterations = 100;
		RpcStrategy strategy = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));

		RpcSender sender = strategy.createSender(pool);
		for (int i = 0; i < iterations; i++) {
			sender.sendRequest(new Object(), 50, new ResultCallbackFuture<>());
		}

		List<RpcSenderStub> connections =
				asList(connection1, connection2, connection3, connection4, connection5);
		for (int i = 0; i < 5; i++) {
			assertEquals(iterations / 5, connections.get(i).getRequests());
		}
	}

	@Test
	public void testCombination2() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		// we don't put connection3
		pool.put(ADDRESS_4, connection4);
		ResultCallbackStub callback = new ResultCallbackStub();
		int iterations = 20;
		RpcStrategy strategy = roundRobin(
				firstAvailable(servers(ADDRESS_1, ADDRESS_2)),
				firstAvailable(servers(ADDRESS_3, ADDRESS_4)));

		RpcSender sender = strategy.createSender(pool);
		for (int i = 0; i < iterations; i++) {
			sender.sendRequest(new Object(), 50, callback);
		}

		assertEquals(iterations / 2, connection1.getRequests());
		assertEquals(0, connection2.getRequests());
		assertEquals(0, connection3.getRequests());
		assertEquals(iterations / 2, connection4.getRequests());
	}

	@Test
	public void testCombination3() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		// we don't put connection2
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		final int shardsCount = 2;
		ShardingFunction<Integer> shardingFunction = new ShardingFunction<Integer>() {
			@Override
			public int getShard(Integer item) {
				return item % shardsCount;
			}
		};
		ResultCallbackStub callback = new ResultCallbackStub();
		RpcStrategy strategy = sharding(shardingFunction,
				firstValidResult(servers(ADDRESS_1, ADDRESS_2)),
				firstValidResult(servers(ADDRESS_3, ADDRESS_4, ADDRESS_5)));

		RpcSender sender = strategy.createSender(pool);
		sender.sendRequest(0, 50, callback);
		sender.sendRequest(0, 50, callback);
		sender.sendRequest(1, 50, callback);
		sender.sendRequest(1, 50, callback);
		sender.sendRequest(0, 50, callback);

		assertEquals(3, connection1.getRequests());
		assertEquals(0, connection2.getRequests());
		assertEquals(2, connection3.getRequests());
		assertEquals(2, connection4.getRequests());
		assertEquals(2, connection5.getRequests());
	}

	@Test
	public void testCombination4() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		HashFunction<Integer> hashFunction = new HashFunction<Integer>() {
			@Override
			public int hashCode(Integer item) {
				return item;
			}
		};
		RpcStrategy strategy = rendezvousHashing(hashFunction)
				.put(1, firstAvailable(servers(ADDRESS_1, ADDRESS_2)))
				.put(2, firstAvailable(servers(ADDRESS_3, ADDRESS_4)))
				.put(3, server(ADDRESS_5));
		int iterationsPerLoop = 1000;
		RpcSender sender;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		sender = strategy.createSender(pool);
		for (int i = 0; i < iterationsPerLoop; i++) {
			sender.sendRequest(i, 50, new ResultCallbackFuture<>());
		}
		pool.remove(ADDRESS_3);
		pool.remove(ADDRESS_4);
		sender = strategy.createSender(pool);
		for (int i = 0; i < iterationsPerLoop; i++) {
			sender.sendRequest(i, 50, new ResultCallbackFuture<>());
		}

		double acceptableError = iterationsPerLoop / 10.0;
		assertEquals(iterationsPerLoop / 3 + iterationsPerLoop / 2, connection1.getRequests(), acceptableError);
		assertEquals(0, connection2.getRequests());
		assertEquals(iterationsPerLoop / 3, connection3.getRequests(), acceptableError);
		assertEquals(0, connection4.getRequests());
		assertEquals(iterationsPerLoop / 3 + iterationsPerLoop / 2, connection5.getRequests(), acceptableError);
	}

	@Test
	public void testCombination5() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();
		int iterationsPerDataStub = 25;
		int iterationsPerDataStubWithKey = 35;
		RpcSender sender;
		RpcStrategy strategy = typeDispatching()
				.on(String.class,
						firstValidResult(servers(ADDRESS_1, ADDRESS_2)))
				.onDefault(
						firstAvailable(servers(ADDRESS_3, ADDRESS_4, ADDRESS_5)));

		sender = strategy.createSender(pool);
		for (int i = 0; i < iterationsPerDataStub; i++) {
			sender.sendRequest(new Object(), timeout, callback);
		}
		for (int i = 0; i < iterationsPerDataStubWithKey; i++) {
			sender.sendRequest("request", timeout, callback);
		}

		assertEquals(iterationsPerDataStubWithKey, connection1.getRequests());
		assertEquals(iterationsPerDataStubWithKey, connection2.getRequests());
		assertEquals(iterationsPerDataStub, connection3.getRequests());
		assertEquals(0, connection4.getRequests());
		assertEquals(0, connection5.getRequests());
	}
}
