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
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class StrategyFirstAvailableTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		// one connection is added
		pool.add(ADDRESS_2, connection);
		RequestSendingStrategy singleServerStrategy1 = new StrategySingleServer(ADDRESS_1);
		RequestSendingStrategy singleServerStrategy2 = new StrategySingleServer(ADDRESS_2);
		RequestSendingStrategy firstAvailableStrategy =
				new StrategyFirstAvailable(asList(singleServerStrategy1, singleServerStrategy2));

		assertFalse(singleServerStrategy1.create(pool).isPresent());
		assertTrue(singleServerStrategy2.create(pool).isPresent());
		assertTrue(firstAvailableStrategy.create(pool).isPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereIsNoActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		// no connections were added to pool
		RequestSendingStrategy singleServerStrategy1 = new StrategySingleServer(ADDRESS_1);
		RequestSendingStrategy singleServerStrategy2 = new StrategySingleServer(ADDRESS_2);
		RequestSendingStrategy singleServerStrategy3 = new StrategySingleServer(ADDRESS_3);
		RequestSendingStrategy firstAvailableStrategy =
				new StrategyFirstAvailable(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));

		assertFalse(singleServerStrategy1.create(pool).isPresent());
		assertFalse(singleServerStrategy2.create(pool).isPresent());
		assertFalse(singleServerStrategy3.create(pool).isPresent());
		assertFalse(firstAvailableStrategy.create(pool).isPresent());
	}

	@Test
	public void itShouldNotBeActiveWhenThereIsNoSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		// no connections were added to pool
		RequestSendingStrategy firstAvailableStrategy =
				new StrategyFirstAvailable(new ArrayList<RequestSendingStrategy>());

		assertFalse(firstAvailableStrategy.create(pool).isPresent());
	}

	@Test
	public void itShouldSendRequestToFirstAvailableSubSender() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RequestSendingStrategy singleServerStrategy1 = new StrategySingleServer(ADDRESS_1);
		RequestSendingStrategy singleServerStrategy2 = new StrategySingleServer(ADDRESS_2);
		RequestSendingStrategy singleServerStrategy3 = new StrategySingleServer(ADDRESS_3);
		RequestSendingStrategy firstAvailableStrategy =
				new StrategyFirstAvailable(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));
		RequestSender senderToFirst;
		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsToSender1 = 10;
		int callsToSender2 = 25;
		int callsToSender3 = 32;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderToFirst = firstAvailableStrategy.create(pool).get();
		for (int i = 0; i < callsToSender1; i++) {
			senderToFirst.sendRequest(data, timeout, callback);
		}
		pool.remove(ADDRESS_1);
		// we should recreate sender after changing in pool
		senderToFirst = firstAvailableStrategy.create(pool).get();
		for (int i = 0; i < callsToSender2; i++) {
			senderToFirst.sendRequest(data, timeout, callback);
		}
		pool.remove(ADDRESS_2);
		// we should recreate sender after changing in pool
		senderToFirst = firstAvailableStrategy.create(pool).get();
		for (int i = 0; i < callsToSender3; i++) {
			senderToFirst.sendRequest(data, timeout, callback);
		}

		assertEquals(callsToSender1, connection1.getCallsAmount());
		assertEquals(callsToSender2, connection2.getCallsAmount());
		assertEquals(callsToSender3, connection3.getCallsAmount());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenSubSendersListIsNull() {
		RequestSendingStrategy strategy = new StrategyFirstAvailable(null);
	}
}
