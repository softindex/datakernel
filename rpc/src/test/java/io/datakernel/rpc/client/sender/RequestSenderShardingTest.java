package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.*;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RequestSenderShardingTest {

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

		HashFunction<RpcMessage.RpcMessageData> hashFunction = new RpcMessageDataStubWithKeyHashFunction();

		RequestSendingStrategy singleServerStrategy1 = new SingleServerStrategy(ADDRESS_1);
		RequestSendingStrategy singleServerStrategy2 = new SingleServerStrategy(ADDRESS_2);
		RequestSendingStrategy singleServerStrategy3 = new SingleServerStrategy(ADDRESS_3);
		RequestSendingStrategy shardingStrategy =
				new ShardingStrategy(hashFunction,
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));

		RequestSender senderSharding;

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

		HashFunction<RpcMessage.RpcMessageData> hashFunction = new RpcMessageDataStubWithKeyHashFunction();

		RequestSendingStrategy singleServerStrategy1 = new SingleServerStrategy(ADDRESS_1);
		RequestSendingStrategy singleServerStrategy2 = new SingleServerStrategy(ADDRESS_2);
		RequestSendingStrategy singleServerStrategy3 = new SingleServerStrategy(ADDRESS_3);
		RequestSendingStrategy shardingStrategy =
				new ShardingStrategy(hashFunction,
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));

		RequestSender senderSharding;

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
	public void itShouldThrowExceptionWhenSubSendersListIsNull() {
		ShardingStrategy strategy = new ShardingStrategy(new RpcMessageDataStubWithKeyHashFunction(), null);
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenHashFunctionIsNull() {
		ShardingStrategy strategy = new ShardingStrategy(null, new ArrayList<RequestSendingStrategy>());
	}
}
