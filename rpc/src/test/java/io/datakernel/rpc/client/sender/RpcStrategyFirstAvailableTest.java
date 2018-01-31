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

import io.datakernel.async.Callback;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.datakernel.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.datakernel.rpc.client.sender.RpcStrategies.firstAvailable;
import static io.datakernel.rpc.client.sender.RpcStrategies.servers;
import static org.junit.Assert.*;

public class RpcStrategyFirstAvailableTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldSendRequestToFirstAvailableSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy firstAvailableStrategy = firstAvailable(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcSender sender;
		int callsToSender1 = 10;
		int callsToSender2 = 25;
		int callsToSender3 = 32;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		sender = firstAvailableStrategy.createSender(pool);
		for (int i = 0; i < callsToSender1; i++) {
			sender.sendRequest(new Object(), 50, Callback.assertNoCalls());
		}
		pool.remove(ADDRESS_1);
		// we should recreate sender after changing in pool
		sender = firstAvailableStrategy.createSender(pool);
		for (int i = 0; i < callsToSender2; i++) {
			sender.sendRequest(new Object(), 50, Callback.assertNoCalls());
		}
		pool.remove(ADDRESS_2);
		// we should recreate sender after changing in pool
		sender = firstAvailableStrategy.createSender(pool);
		for (int i = 0; i < callsToSender3; i++) {
			sender.sendRequest(new Object(), 50, Callback.assertNoCalls());
		}

		assertEquals(callsToSender1, connection1.getRequests());
		assertEquals(callsToSender2, connection2.getRequests());
		assertEquals(callsToSender3, connection3.getRequests());
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		// one connection is added
		pool.put(ADDRESS_2, connection);
		RpcStrategy firstAvailableStrategy =
				firstAvailable(servers(ADDRESS_1, ADDRESS_2));

		assertTrue(firstAvailableStrategy.createSender(pool) != null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategy firstAvailableStrategy = firstAvailable(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		assertFalse(firstAvailableStrategy.createSender(pool) != null);
	}
}
