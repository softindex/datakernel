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

import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.client.sender.helper.RpcRequestSenderStub;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.datakernel.rpc.client.sender.RpcRequestSendingStrategies.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RpcStrategyRoundRobinTest {

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
	public void itShouldSendRequestUsingRoundRobinAlgorithm() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcRequestSenderStub connection1 = new RpcRequestSenderStub();
		RpcRequestSenderStub connection2 = new RpcRequestSenderStub();
		RpcRequestSenderStub connection3 = new RpcRequestSenderStub();
		RpcRequestSendingStrategy server1 = server(ADDRESS_1);
		RpcRequestSendingStrategy server2 = server(ADDRESS_2);
		RpcRequestSendingStrategy server3 = server(ADDRESS_3);
		RpcRequestSendingStrategy roundRobin = roundRobin(server1, server2, server3);
		RpcRequestSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmount = 5;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		senderRoundRobin = roundRobin.createSender(pool);
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, callback);
		}

		assertEquals(2, connection1.getSendsNumber());
		assertEquals(2, connection2.getSendsNumber());
		assertEquals(1, connection3.getSendsNumber());
	}

	@Test
	public void itShouldNotSendRequestToNonActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcRequestSenderStub connection1 = new RpcRequestSenderStub();
		RpcRequestSenderStub connection2 = new RpcRequestSenderStub();
		RpcRequestSenderStub connection4 = new RpcRequestSenderStub();
		RpcRequestSendingStrategy roundRobinStrategy = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcRequestSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmount = 10;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_4, connection4);
		// we don't add connections for ADDRESS_3 and ADDRESS_5
		senderRoundRobin = roundRobinStrategy.createSender(pool);
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, callback);
		}

		assertEquals(4, connection1.getSendsNumber());
		assertEquals(3, connection2.getSendsNumber());
		assertEquals(3, connection4.getSendsNumber());
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcRequestSenderStub connection = new RpcRequestSenderStub();
		// one connection is added
		pool.put(ADDRESS_2, connection);
		RpcRequestSendingStrategy roundRobin = new RpcStrategyRoundRobin(servers(ADDRESS_1, ADDRESS_2));

		assertTrue(roundRobin.createSender(pool) != null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcRequestSendingStrategy roundRobin = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		assertTrue(roundRobin.createSender(pool) == null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcRequestSenderStub connection1 = new RpcRequestSenderStub();
		RpcRequestSenderStub connection2 = new RpcRequestSenderStub();
		RpcRequestSenderStub connection3 = new RpcRequestSenderStub();
		RpcRequestSendingStrategy roundRobin = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3))
				.withMinActiveSubStrategies(4);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);

		assertTrue(roundRobin.createSender(pool) == null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcRequestSenderStub connection1 = new RpcRequestSenderStub();
		RpcRequestSenderStub connection2 = new RpcRequestSenderStub();
		RpcRequestSendingStrategy roundRobin = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3))
				.withMinActiveSubStrategies(3);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		// we don't add connection3

		assertTrue(roundRobin.createSender(pool) == null);
	}

}
