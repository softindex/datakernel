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
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import org.junit.Test;

import java.net.InetSocketAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

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
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy roundRobinStrategy =
				new RpcStrategyRoundRobin(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));
		RpcRequestSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmount = 5;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderRoundRobin = roundRobinStrategy.create(pool).get();
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, callback);
		}

		assertEquals(2, connection1.getCallsAmount());
		assertEquals(2, connection2.getCallsAmount());
		assertEquals(1, connection3.getCallsAmount());
	}

	@Test
	public void itShouldNotSendRequestToNonActiveSubSenders() {
		RpcClientConnectionPool pool
				= new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy singleServerStrategy4 = new RpcStrategySingleServer(ADDRESS_4);
		RpcRequestSendingStrategy singleServerStrategy5 = new RpcStrategySingleServer(ADDRESS_5);
		RpcRequestSendingStrategy roundRobinStrategy =
				new RpcStrategyRoundRobin(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3,
						singleServerStrategy4, singleServerStrategy5));
		RpcRequestSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmount = 10;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_4, connection4);
		// we don't add connections for ADDRESS_3 and ADDRESS_5
		senderRoundRobin = roundRobinStrategy.create(pool).get();
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, callback);
		}

		assertEquals(4, connection1.getCallsAmount());
		assertEquals(3, connection2.getCallsAmount());
		assertEquals(3, connection4.getCallsAmount());
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		// one connection is added
		pool.add(ADDRESS_2, connection);
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy roundRobin =
				new RpcStrategyRoundRobin(asList(singleServerStrategy1, singleServerStrategy2));

		assertFalse(singleServerStrategy1.create(pool).isPresent());
		assertTrue(singleServerStrategy2.create(pool).isPresent());
		assertTrue(roundRobin.create(pool).isPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		// no connections were added to pool
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy roundRobin =
				new RpcStrategyRoundRobin(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));

		assertFalse(singleServerStrategy1.create(pool).isPresent());
		assertFalse(singleServerStrategy2.create(pool).isPresent());
		assertFalse(singleServerStrategy3.create(pool).isPresent());
		assertFalse(roundRobin.create(pool).isPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy roundRobin =
				new RpcStrategyRoundRobin(
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3)
				).withMinActiveSubStrategies(4);

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);

		assertTrue(singleServerStrategy1.create(pool).isPresent());
		assertTrue(singleServerStrategy2.create(pool).isPresent());
		assertTrue(singleServerStrategy3.create(pool).isPresent());
		assertFalse(roundRobin.create(pool).isPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy roundRobin =
				new RpcStrategyRoundRobin(
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3)
				).withMinActiveSubStrategies(3);

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		// we don't add connection3

		assertTrue(singleServerStrategy1.create(pool).isPresent());
		assertTrue(singleServerStrategy2.create(pool).isPresent());
		assertFalse(singleServerStrategy3.create(pool).isPresent());
		assertFalse(roundRobin.create(pool).isPresent());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenSubStrategiesListIsNull() {
		RpcRequestSendingStrategy strategy = new RpcStrategyRoundRobin(null);
	}
}
