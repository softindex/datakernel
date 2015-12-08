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

import io.datakernel.rpc.client.sender.helper.*;
import io.datakernel.rpc.hash.HashFunction;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.datakernel.rpc.client.sender.RpcStrategies.rendezvousHashing;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RpcStrategyRendezvousHashingTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldDistributeCallsBetweenActiveSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.put(shardId1, server1)
				.put(shardId2, server2)
				.put(shardId3, server3);
		RpcSender sender;
		int callsPerLoop = 10000;
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		sender = rendezvousHashing.createSender(pool);
		for (int i = 0; i < callsPerLoop; i++) {
			sender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, callback);
		}
		pool.remove(ADDRESS_1);
		sender = rendezvousHashing.createSender(pool);
		for (int i = 0; i < callsPerLoop; i++) {
			sender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, callback);
		}
		pool.remove(ADDRESS_3);
		sender = rendezvousHashing.createSender(pool);
		for (int i = 0; i < callsPerLoop; i++) {
			sender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, callback);
		}

		int expectedCallsOfConnection1 = callsPerLoop / 3;
		int expectedCallsOfConnection2 = (callsPerLoop / 3) + (callsPerLoop / 2) + callsPerLoop;
		int expectedCallsOfConnection3 = (callsPerLoop / 3) + (callsPerLoop / 2);
		double delta = callsPerLoop / 30.0;
		assertEquals(expectedCallsOfConnection1, connection1.getSendsNumber(), delta);
		assertEquals(expectedCallsOfConnection2, connection2.getSendsNumber(), delta);
		assertEquals(expectedCallsOfConnection3, connection3.getSendsNumber(), delta);
	}

	@Test
	public void itShouldBeCreatedWhenThereAreAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.put(shardId1, server1)
				.put(shardId2, server2)
				.put(shardId3, server3);

		// server3 is active
		pool.put(ADDRESS_3, connection3);

		assertTrue(rendezvousHashing.createSender(pool) != null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.put(shardId1, server1)
				.put(shardId2, server2)
				.put(shardId3, server3);

		// no connections were added to pool, so there are no active servers

		assertTrue(rendezvousHashing.createSender(pool) == null);
	}

	@Test
	public void itShouldNotBeCreatedWhenNoSendersWereAdded() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction);

		assertTrue(rendezvousHashing.createSender(pool) == null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.withMinActiveShards(4)
				.put(shardId1, server1)
				.put(shardId2, server2)
				.put(shardId3, server3);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);

		assertTrue(server1.createSender(pool) != null);
		assertTrue(server2.createSender(pool) != null);
		assertTrue(server3.createSender(pool) != null);
		assertTrue(rendezvousHashing.createSender(pool) == null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.withMinActiveShards(4)
				.put(shardId1, server1)
				.put(shardId2, server2)
				.put(shardId3, server3);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		// we don't add connection3

		assertTrue(server1.createSender(pool) != null);
		assertTrue(server2.createSender(pool) != null);
		assertTrue(server3.createSender(pool) == null);
		assertTrue(rendezvousHashing.createSender(pool) == null);
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenHashFunctionIsNull() {
		RpcStrategy rendezvousHashing = rendezvousHashing(null);
	}
}
