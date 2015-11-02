package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.sender.helper.*;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestSenderTypeDispatcherTest {

	final boolean ACTIVE = true;
	final boolean NON_ACTIVE = false;

	@Test
	public void itShouldChooseSubSenderDependingOnRpcMessageDataType() {

		RequestSenderStub sender1 = new RequestSenderStub(1, ACTIVE);
		RequestSenderStub sender2 = new RequestSenderStub(2, ACTIVE);
		RequestSenderStub sender3 = new RequestSenderStub(3, ACTIVE);

		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> typeToSender = new HashMap<>();

		typeToSender.put(RpcMessageDataTypeOne.class, sender1);
		typeToSender.put(RpcMessageDataTypeTwo.class, sender2);
		typeToSender.put(RpcMessageDataTypeThree.class, sender3);

		int dataTypeOneRequests = 1;
		int dataTypeTwoRequests = 2;
		int dataTypeThreeRequests = 5;

		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		RequestSenderTypeDispatcher senderDispatcher = new RequestSenderTypeDispatcher(typeToSender, null);




		for (int i = 0; i < dataTypeOneRequests; i++) {
			senderDispatcher.sendRequest(new RpcMessageDataTypeOne(), timeout, callback);
		}


		for (int i = 0; i < dataTypeTwoRequests; i++) {
			senderDispatcher.sendRequest(new RpcMessageDataTypeTwo(), timeout, callback);
		}


		for (int i = 0; i < dataTypeThreeRequests; i++) {
			senderDispatcher.sendRequest(new RpcMessageDataTypeThree(), timeout, callback);
		}



		assertEquals(dataTypeOneRequests, sender1.getSendRequestCalls());
		assertEquals(dataTypeTwoRequests, sender2.getSendRequestCalls());
		assertEquals(dataTypeThreeRequests, sender3.getSendRequestCalls());

	}

	@Test
	public void itShouldChooseDefaultSubSenderWhenThereIsNoSpecifiedSubSenderForCurrentDataType() {

		RequestSenderStub sender1 = new RequestSenderStub(1, ACTIVE);
		RequestSenderStub sender2 = new RequestSenderStub(2, ACTIVE);
		RequestSenderStub sender3 = new RequestSenderStub(3, ACTIVE);
		RequestSenderStub defaultSender = new RequestSenderStub(4, ACTIVE);

		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> typeToSender = new HashMap<>();

		typeToSender.put(RpcMessageDataTypeOne.class, sender1);
		typeToSender.put(RpcMessageDataTypeTwo.class, sender2);
		typeToSender.put(RpcMessageDataTypeThree.class, sender3);

		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();

		RequestSenderTypeDispatcher senderDispatcher = new RequestSenderTypeDispatcher(typeToSender, defaultSender);



		senderDispatcher.sendRequest(new RpcMessageDataStub(), timeout, callback);


		assertEquals(0, sender1.getSendRequestCalls());
		assertEquals(0, sender2.getSendRequestCalls());
		assertEquals(0, sender3.getSendRequestCalls());
		assertEquals(1, defaultSender.getSendRequestCalls());

	}

	@Test
	public void itShouldRaiseRpcSenderNotSpecifiedExceptionWhenSenderForDataIsNotSpecifiedAndDefaultSenderIsNull() {

		final AtomicReference<Exception> raisedException = new AtomicReference<>(null);

		RequestSenderStub sender1 = new RequestSenderStub(1, ACTIVE);
		RequestSenderStub sender2 = new RequestSenderStub(2, ACTIVE);
		RequestSenderStub sender3 = new RequestSenderStub(3, ACTIVE);

		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> typeToSender = new HashMap<>();

		typeToSender.put(RpcMessageDataTypeOne.class, sender1);
		typeToSender.put(RpcMessageDataTypeTwo.class, sender2);
		typeToSender.put(RpcMessageDataTypeThree.class, sender3);

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


		RequestSenderTypeDispatcher senderDispatcher = new RequestSenderTypeDispatcher(typeToSender, null);

		// sender is not specified for RpcMessageDataStub, default sender is null
		senderDispatcher.sendRequest(new RpcMessageDataStub(), timeout, callback);

		assertEquals(RpcSenderNotSpecifiedException.class, raisedException.get().getClass());
	}


	@Test
	public void itShouldBeActiveWhenThereIsAtLeastOneActiveSubSender() {
		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> typeToSender = new HashMap<>();
		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> emptyMap = new HashMap<>();

		typeToSender.put(RpcMessageDataTypeOne.class, new RequestSenderStub(1, NON_ACTIVE));
		typeToSender.put(RpcMessageDataTypeTwo.class, new RequestSenderStub(2, ACTIVE));
		typeToSender.put(RpcMessageDataTypeThree.class, new RequestSenderStub(3, NON_ACTIVE));

		RequestSenderTypeDispatcher senderDispatcherOne = new RequestSenderTypeDispatcher(typeToSender, null);

		RequestSenderTypeDispatcher senderDispatcherTwo
				= new RequestSenderTypeDispatcher(emptyMap, new RequestSenderStub(4, ACTIVE));

		assertTrue(senderDispatcherOne.isActive());
		assertTrue(senderDispatcherTwo.isActive());
	}

	@Test
	public void itShouldNotBeActiveWhenThereIsNoActiveSubSenders() {
		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> typeToSender = new HashMap<>();
		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> emptyMap = new HashMap<>();
		typeToSender.put(RpcMessageDataTypeOne.class, new RequestSenderStub(1, NON_ACTIVE));
		typeToSender.put(RpcMessageDataTypeTwo.class, new RequestSenderStub(2, NON_ACTIVE));
		typeToSender.put(RpcMessageDataTypeThree.class, new RequestSenderStub(3, NON_ACTIVE));

		RequestSenderTypeDispatcher senderDispatcherOne = new RequestSenderTypeDispatcher(typeToSender, null);

		RequestSenderTypeDispatcher senderDispatcherTwo
				= new RequestSenderTypeDispatcher(typeToSender, new RequestSenderStub(4, NON_ACTIVE));

		RequestSenderTypeDispatcher senderDispatcherThree
				= new RequestSenderTypeDispatcher(emptyMap, new RequestSenderStub(5, NON_ACTIVE));

		RequestSenderTypeDispatcher senderDispatcherFour
				= new RequestSenderTypeDispatcher(emptyMap, null);

		assertFalse(senderDispatcherOne.isActive());
		assertFalse(senderDispatcherTwo.isActive());
		assertFalse(senderDispatcherThree.isActive());
		assertFalse(senderDispatcherFour.isActive());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenMapIsNull() {
		RequestSenderTypeDispatcher sender = new RequestSenderTypeDispatcher(null, null);
	}



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
