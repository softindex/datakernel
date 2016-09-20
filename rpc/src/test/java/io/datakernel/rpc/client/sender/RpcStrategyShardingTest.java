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
import io.datakernel.rpc.hash.ShardingFunction;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.datakernel.rpc.client.sender.RpcStrategies.servers;
import static io.datakernel.rpc.client.sender.RpcStrategies.sharding;
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
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		final int shardsAmount = 3;
		ShardingFunction<Integer> shardingFunction = new ShardingFunction<Integer>() {
			@Override
			public int getShard(Integer item) {
				return item % shardsAmount;
			}
		};
		RpcStrategy shardingStrategy = sharding(shardingFunction,
				servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcSender senderSharding;
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		senderSharding = shardingStrategy.createSender(pool);
		senderSharding.sendRequest(0, timeout, callback);
		senderSharding.sendRequest(0, timeout, callback);
		senderSharding.sendRequest(1, timeout, callback);
		senderSharding.sendRequest(0, timeout, callback);
		senderSharding.sendRequest(2, timeout, callback);
		senderSharding.sendRequest(0, timeout, callback);
		senderSharding.sendRequest(0, timeout, callback);
		senderSharding.sendRequest(2, timeout, callback);

		assertEquals(5, connection1.getRequests());
		assertEquals(1, connection2.getRequests());
		assertEquals(2, connection3.getRequests());
	}

	@Test(expected = Exception.class)
	public void itShouldCallOnExceptionOfCallbackWhenChosenServerIsNotActive() throws ExecutionException, InterruptedException {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		final int shardsAmount = 3;
		ShardingFunction<Integer> shardingFunction = new ShardingFunction<Integer>() {
			@Override
			public int getShard(Integer item) {
				return item % shardsAmount;
			}
		};
		RpcStrategy shardingStrategy = sharding(shardingFunction,
				servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		// we don't add connection for ADDRESS_1
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		RpcSender sender = shardingStrategy.createSender(pool);

		ResultCallbackFuture<Object> callback1 = ResultCallbackFuture.create();
		ResultCallbackFuture<Object> callback2 = ResultCallbackFuture.create();
		ResultCallbackFuture<Object> callback3 = ResultCallbackFuture.create();

		sender.sendRequest(0, 50, callback1);
		sender.sendRequest(1, 50, callback2);
		sender.sendRequest(2, 50, callback3);

		assertEquals(1, connection2.getRequests());
		assertEquals(1, connection3.getRequests());
		callback1.get();

	}

}
