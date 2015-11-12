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
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.rpc.client.sender.RpcStrategyRendezvousHashing.DefaultBucketHashFunction;
import static io.datakernel.rpc.client.sender.RpcStrategyRendezvousHashing.RendezvousHashBucket;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

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
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy rendezvousHashingStrategy =
				new RpcStrategyRendezvousHashing(hashFunction)
						.put(shardId1, strategySingleServer1)
						.put(shardId2, strategySingleServer2)
						.put(shardId3, strategySingleServer3);
		RpcRequestSender rendezvousSender;
		int callsPerLoop = 10000;
		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		rendezvousSender = rendezvousHashingStrategy.create(pool).getSender();
		for (int i = 0; i < callsPerLoop; i++) {
			rendezvousSender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, callback);
		}
		pool.remove(ADDRESS_1);
		rendezvousSender = rendezvousHashingStrategy.create(pool).getSender();
		for (int i = 0; i < callsPerLoop; i++) {
			rendezvousSender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, callback);
		}
		pool.remove(ADDRESS_3);
		rendezvousSender = rendezvousHashingStrategy.create(pool).getSender();
		for (int i = 0; i < callsPerLoop; i++) {
			rendezvousSender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, callback);
		}

		int expectedCallsOfConnection1 = callsPerLoop / 3;
		int expectedCallsOfConnection2 = (callsPerLoop / 3) + (callsPerLoop / 2) + callsPerLoop;
		int expectedCallsOfConnection3 = (callsPerLoop / 3) + (callsPerLoop / 2);
		double delta = callsPerLoop / 20.0;
		assertEquals(expectedCallsOfConnection1, connection1.getCallsAmount(), delta);
		assertEquals(expectedCallsOfConnection2, connection2.getCallsAmount(), delta);
		assertEquals(expectedCallsOfConnection3, connection3.getCallsAmount(), delta);

	}

	@Test
	public void itShouldBeCreatedWhenThereAreAtLeastOneActiveSubSender() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy rendezvousHashingStrategy =
				new RpcStrategyRendezvousHashing(hashFunction)
						.put(shardId1, strategySingleServer1)
						.put(shardId2, strategySingleServer2)
						.put(shardId3, strategySingleServer3);

		// server3 is active
		pool.add(ADDRESS_3, connection3);

		assertTrue(rendezvousHashingStrategy.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy rendezvousHashingStrategy =
				new RpcStrategyRendezvousHashing(hashFunction)
						.put(shardId1, strategySingleServer1)
						.put(shardId2, strategySingleServer2)
						.put(shardId3, strategySingleServer3);

		// no connections were added to pool, so there are no active servers

		assertFalse(rendezvousHashingStrategy.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenNoSendersWereAdded() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcRequestSendingStrategy rendezvousHashingStrategy = new RpcStrategyRendezvousHashing(hashFunction);

		assertFalse(rendezvousHashingStrategy.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy firstAvailableStrategy =
				new RpcStrategyRendezvousHashing(hashFunction)
						.withMinActiveSubStrategies(4)
						.put(shardId1, strategySingleServer1)
						.put(shardId2, strategySingleServer2)
						.put(shardId3, strategySingleServer3);

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);

		assertTrue(strategySingleServer1.create(pool).isSenderPresent());
		assertTrue(strategySingleServer2.create(pool).isSenderPresent());
		assertTrue(strategySingleServer3.create(pool).isSenderPresent());
		assertFalse(firstAvailableStrategy.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer strategySingleServer1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcStrategySingleServer strategySingleServer2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcStrategySingleServer strategySingleServer3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy firstAvailableStrategy =
				new RpcStrategyRendezvousHashing(hashFunction)
						.withMinActiveSubStrategies(4)
						.put(shardId1, strategySingleServer1)
						.put(shardId2, strategySingleServer2)
						.put(shardId3, strategySingleServer3);

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		// we don't add connection3

		assertTrue(strategySingleServer1.create(pool).isSenderPresent());
		assertTrue(strategySingleServer2.create(pool).isSenderPresent());
		assertFalse(strategySingleServer3.create(pool).isSenderPresent());
		assertFalse(firstAvailableStrategy.create(pool).isSenderPresent());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenHashFunctionIsNull() {
		RpcRequestSendingStrategy rendezvousHashingStrategy = new RpcStrategyRendezvousHashing(null);
	}

	@Test
	public void testRendezvousHashBucket() {
		final int SENDERS_AMOUNT = 4;
		final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
		final Map<Object, RpcRequestSenderHolder> keyToSenderHolder = new HashMap<>(SENDERS_AMOUNT);
		for (int i = 0; i < SENDERS_AMOUNT; i++) {
			keyToSenderHolder.put(i, RpcRequestSenderHolder.of(new RequestSenderStub(i)));
		}
		RendezvousHashBucket hashBucket;

		hashBucket = RendezvousHashBucket.create(keyToSenderHolder, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		RpcRequestSender[] baseBucket = new RpcRequestSender[DEFAULT_BUCKET_CAPACITY];
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			baseBucket[i] = hashBucket.chooseSender(i);
		}

		int key1 = 1;
		RpcRequestSender sender1 = keyToSenderHolder.get(key1).getSender();
		keyToSenderHolder.put(key1, RpcRequestSenderHolder.absent());
		hashBucket = RendezvousHashBucket.create(keyToSenderHolder, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			if (!baseBucket[i].equals(sender1))
				assertEquals(baseBucket[i], hashBucket.chooseSender(i));
			else
				assertFalse(hashBucket.chooseSender(i).equals(sender1));
		}

		int key2 = 2;
		RpcRequestSender sender2 = keyToSenderHolder.get(key2).getSender();
		keyToSenderHolder.put(key2, RpcRequestSenderHolder.absent());
		hashBucket = RendezvousHashBucket.create(keyToSenderHolder, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			if (!baseBucket[i].equals(sender1) && !baseBucket[i].equals(sender2))
				assertEquals(baseBucket[i], hashBucket.chooseSender(i));
			else
				assertFalse(hashBucket.chooseSender(i).equals(sender1));
			assertFalse(hashBucket.chooseSender(i).equals(sender2));
		}

		keyToSenderHolder.put(key1, RpcRequestSenderHolder.of(new RequestSenderStub(key1)));
		hashBucket = RendezvousHashBucket.create(keyToSenderHolder, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			if (!baseBucket[i].equals(sender2))
				assertEquals(baseBucket[i], hashBucket.chooseSender(i));
			else
				assertFalse(hashBucket.chooseSender(i).equals(sender2));
		}

		keyToSenderHolder.put(key2, RpcRequestSenderHolder.of(new RequestSenderStub(key2)));
		hashBucket = RendezvousHashBucket.create(keyToSenderHolder, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			assertEquals(baseBucket[i], hashBucket.chooseSender(i));
		}
	}
}
