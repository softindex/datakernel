package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.*;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestSenderTypeDispatcherTest {

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
	public void itShouldChooseSubSenderDependingOnRpcMessageDataType() {

		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);

		SingleServerStrategy singleServerStrategy1 = new SingleServerStrategy(ADDRESS_1);
		SingleServerStrategy singleServerStrategy2 = new SingleServerStrategy(ADDRESS_2);
		SingleServerStrategy singleServerStrategy3 = new SingleServerStrategy(ADDRESS_3);
		RequestSendingStrategy typeDispatchingStrategy = new TypeDispatchingStrategy()
				.on(RpcMessageDataTypeOne.class, singleServerStrategy1)
				.on(RpcMessageDataTypeTwo.class, singleServerStrategy2)
				.on(RpcMessageDataTypeThree.class, singleServerStrategy3);

		int dataTypeOneRequests = 1;
		int dataTypeTwoRequests = 2;
		int dataTypeThreeRequests = 5;

		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		RequestSender senderDispatcher = typeDispatchingStrategy.create(pool).get();



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
	public void itShouldChooseDefaultSubSenderWhenThereIsNoSpecifiedSubSenderForCurrentDataType() {

		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4));

		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		pool.add(ADDRESS_4, connection4);

		SingleServerStrategy singleServerStrategy1 = new SingleServerStrategy(ADDRESS_1);
		SingleServerStrategy singleServerStrategy2 = new SingleServerStrategy(ADDRESS_2);
		SingleServerStrategy singleServerStrategy3 = new SingleServerStrategy(ADDRESS_3);
		SingleServerStrategy singleServerStrategy4 = new SingleServerStrategy(ADDRESS_4);
		RequestSendingStrategy typeDispatchingStrategy = new TypeDispatchingStrategy()
				.on(RpcMessageDataTypeOne.class, singleServerStrategy1)
				.on(RpcMessageDataTypeTwo.class, singleServerStrategy2)
				.on(RpcMessageDataTypeThree.class, singleServerStrategy3)
				.onDefault(singleServerStrategy4);

		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		RequestSender senderDispatcher = typeDispatchingStrategy.create(pool).get();



		senderDispatcher.sendRequest(new RpcMessageDataStub(), timeout, callback);


		assertEquals(0, connection1.getCallsAmount());
		assertEquals(0, connection2.getCallsAmount());
		assertEquals(0, connection3.getCallsAmount());
		assertEquals(1, connection4.getCallsAmount());  // connection of default server

	}

	@Test
	public void itShouldRaiseExceptionWhenSenderForDataIsNotSpecifiedAndDefaultSenderIsNull() {

		final AtomicReference<Exception> raisedException = new AtomicReference<>(null);

		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);

		SingleServerStrategy singleServerStrategy1 = new SingleServerStrategy(ADDRESS_1);
		SingleServerStrategy singleServerStrategy2 = new SingleServerStrategy(ADDRESS_2);
		SingleServerStrategy singleServerStrategy3 = new SingleServerStrategy(ADDRESS_3);
		RequestSendingStrategy typeDispatchingStrategy = new TypeDispatchingStrategy()
				.on(RpcMessageDataTypeOne.class, singleServerStrategy1)
				.on(RpcMessageDataTypeTwo.class, singleServerStrategy2)
				.on(RpcMessageDataTypeThree.class, singleServerStrategy3);

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


		RequestSender senderDispatcher = typeDispatchingStrategy.create(pool).get();

		// sender is not specified for RpcMessageDataStub, default sender is null
		senderDispatcher.sendRequest(new RpcMessageDataStub(), timeout, callback);

		assertEquals(RpcNoSenderAvailableException.class, raisedException.get().getClass());
	}


//	TODO (vmykhalko): add tests to check whether sender is created when there is at least one absent mandatory server
//	@Test
//	public void itShouldBeActiveWhenThereIsAtLeastOneActiveSubSender() {
//		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> typeToSender = new HashMap<>();
//		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> emptyMap = new HashMap<>();
//
//		typeToSender.put(RpcMessageDataTypeOne.class, new RequestSenderStub(1, NON_ACTIVE));
//		typeToSender.put(RpcMessageDataTypeTwo.class, new RequestSenderStub(2, ACTIVE));
//		typeToSender.put(RpcMessageDataTypeThree.class, new RequestSenderStub(3, NON_ACTIVE));
//
//		RequestSenderTypeDispatcher senderDispatcherOne = new RequestSenderTypeDispatcher(typeToSender, null);
//
//		RequestSenderTypeDispatcher senderDispatcherTwo
//				= new RequestSenderTypeDispatcher(emptyMap, new RequestSenderStub(4, ACTIVE));
//
//		assertTrue(senderDispatcherOne.isActive());
//		assertTrue(senderDispatcherTwo.isActive());
//	}
//
//	@Test
//	public void itShouldNotBeActiveWhenThereIsNoActiveSubSenders() {
//		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> typeToSender = new HashMap<>();
//		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> emptyMap = new HashMap<>();
//		typeToSender.put(RpcMessageDataTypeOne.class, new RequestSenderStub(1, NON_ACTIVE));
//		typeToSender.put(RpcMessageDataTypeTwo.class, new RequestSenderStub(2, NON_ACTIVE));
//		typeToSender.put(RpcMessageDataTypeThree.class, new RequestSenderStub(3, NON_ACTIVE));
//
//		RequestSenderTypeDispatcher senderDispatcherOne = new RequestSenderTypeDispatcher(typeToSender, null);
//
//		RequestSenderTypeDispatcher senderDispatcherTwo
//				= new RequestSenderTypeDispatcher(typeToSender, new RequestSenderStub(4, NON_ACTIVE));
//
//		RequestSenderTypeDispatcher senderDispatcherThree
//				= new RequestSenderTypeDispatcher(emptyMap, new RequestSenderStub(5, NON_ACTIVE));
//
//		RequestSenderTypeDispatcher senderDispatcherFour
//				= new RequestSenderTypeDispatcher(emptyMap, null);
//
//		assertFalse(senderDispatcherOne.isActive());
//		assertFalse(senderDispatcherTwo.isActive());
//		assertFalse(senderDispatcherThree.isActive());
//		assertFalse(senderDispatcherFour.isActive());
//	}




	static class RpcMessageDataTypeOne implements RpcMessage.RpcMessageData {

		@Override
		public boolean isMandatory() {
			return false;
		}
	}

	static class RpcMessageDataTypeTwo implements RpcMessage.RpcMessageData {

		@Override
		public boolean isMandatory() {
			return false;
		}
	}

	static class RpcMessageDataTypeThree implements RpcMessage.RpcMessageData {

		@Override
		public boolean isMandatory() {
			return false;
		}
	}
}
