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
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RpcStrategyTypeDispatchingTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final int PORT_4 = 10004;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);
	private static final InetSocketAddress ADDRESS_4 = new InetSocketAddress(HOST, PORT_4);

	@Test
	public void itShouldChooseSubStrategyDependingOnRpcMessageDataType() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy typeDispatchingStrategy = RpcStrategyTypeDispatching.create()
				.on(RpcMessageDataTypeOne.class, server1)
				.on(RpcMessageDataTypeTwo.class, server2)
				.on(RpcMessageDataTypeThree.class, server3);
		int dataTypeOneRequests = 1;
		int dataTypeTwoRequests = 2;
		int dataTypeThreeRequests = 5;

		RpcSender sender = typeDispatchingStrategy.createSender(pool);
		for (int i = 0; i < dataTypeOneRequests; i++) {
			sender.sendRequest(new RpcMessageDataTypeOne(), 50, ResultCallbackFuture.create());
		}
		for (int i = 0; i < dataTypeTwoRequests; i++) {
			sender.sendRequest(new RpcMessageDataTypeTwo(), 50, ResultCallbackFuture.create());
		}
		for (int i = 0; i < dataTypeThreeRequests; i++) {
			sender.sendRequest(new RpcMessageDataTypeThree(), 50, ResultCallbackFuture.create());
		}

		assertEquals(dataTypeOneRequests, connection1.getRequests());
		assertEquals(dataTypeTwoRequests, connection2.getRequests());
		assertEquals(dataTypeThreeRequests, connection3.getRequests());
	}

	@Test
	public void itShouldChooseDefaultSubStrategyWhenThereIsNoSpecifiedSubSenderForCurrentDataType() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategySingleServer defaultServer = server(ADDRESS_4);
		RpcStrategy typeDispatchingStrategy = RpcStrategyTypeDispatching.create()
				.on(RpcMessageDataTypeOne.class, server1)
				.on(RpcMessageDataTypeTwo.class, server2)
				.on(RpcMessageDataTypeThree.class, server3)
				.onDefault(defaultServer);
		ResultCallbackStub callback = new ResultCallbackStub();

		RpcSender sender = typeDispatchingStrategy.createSender(pool);
		sender.sendRequest(new RpcMessageDataStub(), 50, callback);

		assertEquals(0, connection1.getRequests());
		assertEquals(0, connection2.getRequests());
		assertEquals(0, connection3.getRequests());
		assertEquals(1, connection4.getRequests());  // connection of default server

	}

	@Test(expected = ExecutionException.class)
	public void itShouldRaiseExceptionWhenStrategyForDataIsNotSpecifiedAndDefaultSenderIsNull() throws ExecutionException, InterruptedException {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		RpcStrategySingleServer server1 = RpcStrategySingleServer.create(ADDRESS_1);
		RpcStrategySingleServer server2 = RpcStrategySingleServer.create(ADDRESS_2);
		RpcStrategySingleServer server3 = RpcStrategySingleServer.create(ADDRESS_3);
		RpcStrategy typeDispatchingStrategy = RpcStrategyTypeDispatching.create()
				.on(RpcMessageDataTypeOne.class, server1)
				.on(RpcMessageDataTypeTwo.class, server2)
				.on(RpcMessageDataTypeThree.class, server3);

		RpcSender sender = typeDispatchingStrategy.createSender(pool);
		// sender is not specified for RpcMessageDataStub, default sender is null
		ResultCallbackFuture<Object> callback = ResultCallbackFuture.create();
		sender.sendRequest(new RpcMessageDataStub(), 50, callback);

		callback.get();
	}

	@Test
	public void itShouldNotBeCreatedWhenAtLeastOneOfCrucialSubStrategyIsNotActive() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategySingleServer server1 = RpcStrategySingleServer.create(ADDRESS_1);
		RpcStrategySingleServer server2 = RpcStrategySingleServer.create(ADDRESS_2);
		RpcStrategySingleServer server3 = RpcStrategySingleServer.create(ADDRESS_3);
		RpcStrategy typeDispatchingStrategy = RpcStrategyTypeDispatching.create()
				.on(RpcMessageDataTypeOne.class, server1)
				.on(RpcMessageDataTypeTwo.class, server2)
				.on(RpcMessageDataTypeThree.class, server3);

		pool.put(ADDRESS_1, connection1);
		// we don't put connection 2
		pool.put(ADDRESS_3, connection3);

		assertTrue(typeDispatchingStrategy.createSender(pool) == null);
	}

	@Test
	public void itShouldNotBeCreatedWhenDefaultStrategyIsNotActiveAndCrucial() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategySingleServer server1 = RpcStrategySingleServer.create(ADDRESS_1);
		RpcStrategySingleServer server2 = RpcStrategySingleServer.create(ADDRESS_2);
		RpcStrategySingleServer server3 = RpcStrategySingleServer.create(ADDRESS_3);
		RpcStrategySingleServer defaultServer = RpcStrategySingleServer.create(ADDRESS_4);
		RpcStrategy typeDispatchingStrategy = RpcStrategyTypeDispatching.create()
				.on(RpcMessageDataTypeOne.class, server1)
				.on(RpcMessageDataTypeTwo.class, server2)
				.on(RpcMessageDataTypeThree.class, server3)
				.onDefault(defaultServer);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		// we don't add connection for default server

		assertTrue(typeDispatchingStrategy.createSender(pool) == null);
	}

	static class RpcMessageDataTypeOne {

	}

	static class RpcMessageDataTypeTwo {

	}

	static class RpcMessageDataTypeThree {

	}
}
