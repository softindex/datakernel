package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RequestSenderToAllTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldSendRequestToAllAvailableSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();

		RequestSender senderToServer1;
		RequestSender senderToServer2;
		RequestSender senderToServer3;
		RequestSenderToAll senderToAll;

		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();

		int callsAmountIterationOne = 10;
		int callsAmountIterationTwo = 25;




		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		senderToAll = new RequestSenderToAll(asList(senderToServer1, senderToServer2, senderToServer3));
		for (int i = 0; i < callsAmountIterationOne; i++) {
			senderToAll.sendRequest(data, timeout, callback);
		}

		pool.remove(ADDRESS_1);
		// we should recreate sender after changing in pool
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		senderToAll = new RequestSenderToAll(asList(senderToServer1, senderToServer2, senderToServer3));
		for (int i = 0; i < callsAmountIterationTwo; i++) {
			senderToAll.sendRequest(data, timeout, callback);
		}




		assertEquals(callsAmountIterationOne, connection1.getCallsAmount());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection2.getCallsAmount());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection3.getCallsAmount());
	}

	@Test
	public void itShouldCallOnResultWithNullIfAllSendersReturnedNull() {

		final AtomicInteger onResultWithNullWasCalledTimes = new AtomicInteger(0);

		RequestSender sender1 = new RequestSenderOnResultWithNullCaller();
		RequestSender sender2 = new RequestSenderOnResultWithNullCaller();
		RequestSender sender3 = new RequestSenderOnResultWithNullCaller();
		RequestSenderToAll senderToAll = new RequestSenderToAll(asList(sender1, sender2, sender3));

		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallback<RpcMessageDataStub> callback = new ResultCallback<RpcMessageDataStub>() {
			@Override
			public void onException(Exception exception) {
				throw new IllegalStateException();
			}

			@Override
			public void onResult(RpcMessageDataStub result) {
				onResultWithNullWasCalledTimes.incrementAndGet();
			}
		};

		senderToAll.sendRequest(data, timeout, callback);

		// despite there are several sender, onResult should be called only once after all senders returned null
		assertEquals(1, onResultWithNullWasCalledTimes.get());
	}

	@Test
	public void itShouldCallOnResultWithValueIfAtLeastOneSendersReturnedValue() {

		final AtomicInteger onResultWithNullWasCalledTimes = new AtomicInteger(0);
		final AtomicInteger onResultWithValueWasCalledTimes = new AtomicInteger(0);

		RequestSender sender1 = new RequestSenderOnResultWithNullCaller();
		RequestSender sender2 = new RequestSenderOnResultWithNullCaller();
		RequestSender sender3 = new RequestSenderOnResultWithValueCaller();
		RequestSenderToAll senderToAll = new RequestSenderToAll(asList(sender1, sender2, sender3));

		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallback<RpcMessageDataStub> callback = new ResultCallback<RpcMessageDataStub>() {
			@Override
			public void onException(Exception exception) {
				throw new IllegalStateException();
			}

			@Override
			public void onResult(RpcMessageDataStub result) {
				if (result != null) {
					onResultWithValueWasCalledTimes.incrementAndGet();
				} else {
					onResultWithNullWasCalledTimes.incrementAndGet();
				}
			}
		};

		senderToAll.sendRequest(data, timeout, callback);

		assertEquals(1, onResultWithValueWasCalledTimes.get());
		assertEquals(0, onResultWithNullWasCalledTimes.get());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenSubSendersListIsNull() {
		RequestSenderToAll sender = new RequestSenderToAll(null);
	}

	static final class RequestSenderOnResultWithNullCaller implements RequestSender {

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, ResultCallback<T> callback) {
			callback.onResult(null);
		}

		@Override
		public boolean isActive() {
			return true;
		}
	}

	static final class RequestSenderOnResultWithValueCaller implements RequestSender {

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, ResultCallback<T> callback) {
			callback.onResult((T)new RpcMessageDataStub());
		}

		@Override
		public boolean isActive() {
			return true;
		}
	}
}
