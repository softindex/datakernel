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

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStubWithKey;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

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
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy typeDispatchingStrategy = new RpcStrategyTypeDispatching()
				.on(RpcMessageDataTypeOne.class, strategySingleServer1)
				.on(RpcMessageDataTypeTwo.class, strategySingleServer2)
				.on(RpcMessageDataTypeThree.class, strategySingleServer3);
		int dataTypeOneRequests = 1;
		int dataTypeTwoRequests = 2;
		int dataTypeThreeRequests = 5;
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		RpcRequestSender senderDispatcher = typeDispatchingStrategy.create(pool).getSender();
		for (int i = 0; i < dataTypeOneRequests; i++) {
			senderDispatcher.sendRequest(new RpcMessageDataTypeOne(), timeout, callback);
		}
		for (int i = 0; i < dataTypeTwoRequests; i++) {
			senderDispatcher.sendRequest(new RpcMessageDataTypeTwo(), timeout, callback);
		}
		for (int i = 0; i < dataTypeThreeRequests; i++) {
			senderDispatcher.sendRequest(new RpcMessageDataTypeThree(), timeout, callback);
		}

		assertEquals(dataTypeOneRequests, connection1.getCallsAmount());
		assertEquals(dataTypeTwoRequests, connection2.getCallsAmount());
		assertEquals(dataTypeThreeRequests, connection3.getCallsAmount());

	}

	@Test
	public void itShouldChooseDefaultSubStrategyWhenThereIsNoSpecifiedSubSenderForCurrentDataType() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		pool.add(ADDRESS_4, connection4);
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcStrategySingleServer defaultServer = new RpcStrategySingleServer(ADDRESS_4);
		RpcRequestSendingStrategy typeDispatchingStrategy = new RpcStrategyTypeDispatching()
				.on(RpcMessageDataTypeOne.class, strategySingleServer1)
				.on(RpcMessageDataTypeTwo.class, strategySingleServer2)
				.on(RpcMessageDataTypeThree.class, strategySingleServer3)
				.onDefault(defaultServer);
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		RpcRequestSender senderDispatcher = typeDispatchingStrategy.create(pool).getSender();
		senderDispatcher.sendRequest(new RpcMessageDataStub(), timeout, callback);

		assertEquals(0, connection1.getCallsAmount());
		assertEquals(0, connection2.getCallsAmount());
		assertEquals(0, connection3.getCallsAmount());
		assertEquals(1, connection4.getCallsAmount());  // connection of default server

	}

	@Test
	public void itShouldRaiseExceptionWhenStrategyForDataIsNotSpecifiedAndDefaultSenderIsNull() {
		final AtomicReference<Exception> raisedException = new AtomicReference<>(null);
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy typeDispatchingStrategy = new RpcStrategyTypeDispatching()
				.on(RpcMessageDataTypeOne.class, strategySingleServer1)
				.on(RpcMessageDataTypeTwo.class, strategySingleServer2)
				.on(RpcMessageDataTypeThree.class, strategySingleServer3);
		int timeout = 50;
		ResultCallback<RpcMessageDataStubWithKey> callback = new ResultCallback<RpcMessageDataStubWithKey>() {
			@Override
			public void onResult(RpcMessageDataStubWithKey result) {

			}

			@Override
			public void onException(Exception exception) {
				raisedException.compareAndSet(null, exception);
			}
		};

		RpcRequestSender senderDispatcher = typeDispatchingStrategy.create(pool).getSender();
		// sender is not specified for RpcMessageDataStub, default sender is null
		senderDispatcher.sendRequest(new RpcMessageDataStub(), timeout, callback);

		assertEquals(RpcNoSenderAvailableException.class, raisedException.get().getClass());
	}

	@Test
	public void itShouldNotBeCreatedWhenAtLeastOneOfCrucialSubStrategyIsNotActive() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy typeDispatchingStrategy = new RpcStrategyTypeDispatching()
				.on(RpcMessageDataTypeOne.class, strategySingleServer1)
				.on(RpcMessageDataTypeTwo.class, strategySingleServer2).crucialForActivation(true)
				.on(RpcMessageDataTypeThree.class, strategySingleServer3);

		pool.add(ADDRESS_1, connection1);
		// we don't add connection 2
		pool.add(ADDRESS_3, connection3);

		assertTrue(strategySingleServer1.create(pool).isSenderPresent());
		assertFalse(strategySingleServer2.create(pool).isSenderPresent());
		assertTrue(strategySingleServer3.create(pool).isSenderPresent());
		assertFalse(typeDispatchingStrategy.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldBeCreatedWhenAtLeastOneOfSubStrategyIsActiveAndNoneOfStrategiesAreCrucial() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy typeDispatchingStrategy = new RpcStrategyTypeDispatching()
				.on(RpcMessageDataTypeOne.class, strategySingleServer1).crucialForActivation(false)
				.on(RpcMessageDataTypeTwo.class, strategySingleServer2).crucialForActivation(false)
				.on(RpcMessageDataTypeThree.class, strategySingleServer3); // actually is not crucial by default

		pool.add(ADDRESS_1, connection1);
		// we don't add connection 2
		pool.add(ADDRESS_3, connection3);

		assertTrue(strategySingleServer1.create(pool).isSenderPresent());
		assertFalse(strategySingleServer2.create(pool).isSenderPresent());
		assertTrue(strategySingleServer3.create(pool).isSenderPresent());
		assertTrue(typeDispatchingStrategy.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenDefaultStrategyIsNotActiveAndCrucial() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcStrategySingleServer defaultServer = new RpcStrategySingleServer(ADDRESS_4);
		RpcRequestSendingStrategy typeDispatchingStrategy = new RpcStrategyTypeDispatching()
				.on(RpcMessageDataTypeOne.class, strategySingleServer1)
				.on(RpcMessageDataTypeTwo.class, strategySingleServer2)
				.on(RpcMessageDataTypeThree.class, strategySingleServer3)
				.onDefault(defaultServer).crucialForActivation(true);

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		// we don't add connection for default server

		assertTrue(strategySingleServer1.create(pool).isSenderPresent());
		assertTrue(strategySingleServer2.create(pool).isSenderPresent());
		assertTrue(strategySingleServer3.create(pool).isSenderPresent());
		assertFalse(defaultServer.create(pool).isSenderPresent());
		assertFalse(typeDispatchingStrategy.create(pool).isSenderPresent());
	}

	static class RpcMessageDataTypeOne {

	}

	static class RpcMessageDataTypeTwo {

	}

	static class RpcMessageDataTypeThree {

	}
}
