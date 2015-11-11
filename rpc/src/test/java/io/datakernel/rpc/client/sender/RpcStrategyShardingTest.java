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
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStubWithKey;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStubWithKeyHashFunction;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RpcStrategyShardingTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldSelectSubSenderConsideringHashCodeOfRequestData() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy shardingStrategy =
				new RpcStrategySharding(hashFunction,
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));
		RpcRequestSender senderSharding;
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderSharding = shardingStrategy.create(pool).get();
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(0), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(0), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(1), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(0), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(2), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(0), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(0), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(2), timeout, callback);

		assertEquals(5, connection1.getCallsAmount());
		assertEquals(1, connection2.getCallsAmount());
		assertEquals(2, connection3.getCallsAmount());
	}

	@Test
	public void itShouldCallOnExceptionOfCallbackWhenChosenServerIsNotActive() {
		final AtomicInteger onExceptionCallsAmount = new AtomicInteger(0);
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy shardingStrategy =
				new RpcStrategySharding(hashFunction,
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));
		RpcRequestSender senderSharding;
		int timeout = 50;
		ResultCallback<RpcMessageDataStubWithKey> callback = new ResultCallback<RpcMessageDataStubWithKey>() {
			@Override
			public void onResult(RpcMessageDataStubWithKey result) {

			}

			@Override
			public void onException(Exception exception) {
				onExceptionCallsAmount.incrementAndGet();
			}
		};

		// we don't add connection for ADDRESS_1
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderSharding = shardingStrategy.create(pool).get();
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(0), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(1), timeout, callback);
		senderSharding.sendRequest(new RpcMessageDataStubWithKey(2), timeout, callback);

		assertEquals(1, onExceptionCallsAmount.get());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenSubStrategiesListIsNull() {
		RpcStrategySharding strategy = new RpcStrategySharding(new RpcMessageDataStubWithKeyHashFunction(), null);
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenHashFunctionIsNull() {
		RpcStrategySharding strategy = new RpcStrategySharding(null, new ArrayList<RpcRequestSendingStrategy>());
	}
}
