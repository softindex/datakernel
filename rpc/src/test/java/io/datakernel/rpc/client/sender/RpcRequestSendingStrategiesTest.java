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

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.*;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.rpc.client.sender.RpcRequestSendingStrategies.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RpcRequestSendingStrategiesTest {

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
		RpcClientConnectionPool pool =
				new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection5 = new RpcClientConnectionStub();
		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		pool.add(ADDRESS_4, connection4);
		pool.add(ADDRESS_5, connection5);
		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int iterations = 100;
		RpcRequestSendingStrategy strategy =
				roundRobin(
						server(ADDRESS_1),
						server(ADDRESS_2),
						servers(ADDRESS_3, ADDRESS_4, ADDRESS_5)
				);

		RpcRequestSender sender = strategy.create(pool).get();
		for (int i = 0; i < iterations; i++) {
			sender.sendRequest(data, timeout, callback);
		}

		List<RpcClientConnectionStub> connections =
				asList(connection1, connection2, connection3, connection4, connection5);
		for (int i = 0; i < 5; i++) {
			assertEquals(iterations / 5, connections.get(i).getCallsAmount());
		}
	}

	@Test
	public void testCombination2() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		// we don't add connection3
		pool.add(ADDRESS_4, connection4);
		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int iterations = 20;
		RpcRequestSendingStrategy strategy =
				roundRobin(
						firstAvailable(
								servers(ADDRESS_1, ADDRESS_2)
						),
						firstAvailable(
								servers(ADDRESS_3, ADDRESS_4)
						)
				);

		RpcRequestSender sender = strategy.create(pool).get();
		for (int i = 0; i < iterations; i++) {
			sender.sendRequest(data, timeout, callback);
		}

		assertEquals(iterations / 2, connection1.getCallsAmount());
		assertEquals(0, connection2.getCallsAmount());
		assertEquals(0, connection3.getCallsAmount());
		assertEquals(iterations / 2, connection4.getCallsAmount());
	}

	@Test
	public void testCombination3() {
		RpcClientConnectionPool pool =
				new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection5 = new RpcClientConnectionStub();
		pool.add(ADDRESS_1, connection1);
		// we don't add connection2
		pool.add(ADDRESS_3, connection3);
		pool.add(ADDRESS_4, connection4);
		pool.add(ADDRESS_5, connection5);
		HashFunction<RpcMessage.RpcMessageData> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		int timeout = 50;
		RpcMessage.RpcMessageData data0 = new RpcMessageDataStubWithKey(0);
		RpcMessage.RpcMessageData data1 = new RpcMessageDataStubWithKey(1);
		ResultCallbackStub callback = new ResultCallbackStub();
		RpcRequestSendingStrategy strategy =
				sharding(
						hashFunction,
						allAvailable(
								servers(ADDRESS_1, ADDRESS_2)
						),
						allAvailable(
								servers(ADDRESS_3, ADDRESS_4, ADDRESS_5)
						)
				);

		RpcRequestSender sender = strategy.create(pool).get();
		sender.sendRequest(data0, timeout, callback);
		sender.sendRequest(data0, timeout, callback);
		sender.sendRequest(data1, timeout, callback);
		sender.sendRequest(data1, timeout, callback);
		sender.sendRequest(data0, timeout, callback);

		assertEquals(3, connection1.getCallsAmount());
		assertEquals(0, connection2.getCallsAmount());
		assertEquals(2, connection3.getCallsAmount());
		assertEquals(2, connection4.getCallsAmount());
		assertEquals(2, connection5.getCallsAmount());
	}

	@Test
	public void testCombination4() {
		RpcClientConnectionPool pool =
				new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection5 = new RpcClientConnectionStub();
		HashFunction<RpcMessage.RpcMessageData> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcRequestSendingStrategy strategy =
				rendezvousHashing(hashFunction)
						.put(1, firstAvailable(servers(ADDRESS_1, ADDRESS_2)))
						.put(2, firstAvailable(servers(ADDRESS_3, ADDRESS_4)))
						.put(3, server(ADDRESS_5)
						);
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();
		int iterationsPerLoop = 1000;
		RpcRequestSender sender;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		pool.add(ADDRESS_4, connection4);
		pool.add(ADDRESS_5, connection5);
		sender = strategy.create(pool).get();
		for (int i = 0; i < iterationsPerLoop; i++) {
			RpcMessage.RpcMessageData data = new RpcMessageDataStubWithKey(i);
			sender.sendRequest(data, timeout, callback);
		}
		pool.remove(ADDRESS_3);
		pool.remove(ADDRESS_4);
		sender = strategy.create(pool).get();
		for (int i = 0; i < iterationsPerLoop; i++) {
			RpcMessage.RpcMessageData data = new RpcMessageDataStubWithKey(i);
			sender.sendRequest(data, timeout, callback);
		}

		double acceptableError = iterationsPerLoop / 10.0;
		assertEquals(iterationsPerLoop / 3 + iterationsPerLoop / 2, connection1.getCallsAmount(), acceptableError);
		assertEquals(0, connection2.getCallsAmount());
		assertEquals(iterationsPerLoop / 3, connection3.getCallsAmount(), acceptableError);
		assertEquals(0, connection4.getCallsAmount());
		assertEquals(iterationsPerLoop / 3 + iterationsPerLoop / 2, connection5.getCallsAmount(), acceptableError);
	}

	@Test
	public void testCombination5() {
		RpcClientConnectionPool pool =
				new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection5 = new RpcClientConnectionStub();
		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		pool.add(ADDRESS_4, connection4);
		pool.add(ADDRESS_5, connection5);
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();
		int iterationsPerDataStub = 25;
		int iterationsPerDataStubWithKey = 35;
		RpcRequestSender sender;
		RpcRequestSendingStrategy strategy =
				typeDispatching()
						.on(RpcMessageDataStubWithKey.class,
								allAvailable(
										servers(ADDRESS_1, ADDRESS_2)
								)
						)
						.onDefault(
								firstAvailable(
										servers(ADDRESS_3, ADDRESS_4, ADDRESS_5)
								)
						);

		sender = strategy.create(pool).get();
		for (int i = 0; i < iterationsPerDataStub; i++) {
			RpcMessage.RpcMessageData data = new RpcMessageDataStub();
			sender.sendRequest(data, timeout, callback);
		}
		for (int i = 0; i < iterationsPerDataStubWithKey; i++) {
			RpcMessage.RpcMessageData data = new RpcMessageDataStubWithKey(i);
			sender.sendRequest(data, timeout, callback);
		}

		assertEquals(iterationsPerDataStubWithKey, connection1.getCallsAmount());
		assertEquals(iterationsPerDataStubWithKey, connection2.getCallsAmount());
		assertEquals(iterationsPerDataStub, connection3.getCallsAmount());
		assertEquals(0, connection4.getCallsAmount());
		assertEquals(0, connection5.getCallsAmount());
	}
}
