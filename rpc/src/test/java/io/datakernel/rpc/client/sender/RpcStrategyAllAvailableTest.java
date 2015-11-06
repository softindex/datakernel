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
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RpcStrategyAllAvailableTest {

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
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy allAvailableStrategy =
				new RpcStrategyAllAvailable(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));
		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmountIterationOne = 10;
		int callsAmountIterationTwo = 25;
		RpcRequestSender senderToAll;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderToAll = allAvailableStrategy.create(pool).get();
		for (int i = 0; i < callsAmountIterationOne; i++) {
			senderToAll.sendRequest(data, timeout, callback);
		}
		pool.remove(ADDRESS_1);
		// we should recreate sender after changing in pool
		senderToAll = allAvailableStrategy.create(pool).get();
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
		RpcRequestSender sender1 = new RequestSenderOnResultWithNullCaller();
		RpcRequestSender sender2 = new RequestSenderOnResultWithNullCaller();
		RpcRequestSender sender3 = new RequestSenderOnResultWithNullCaller();
		RpcStrategyAllAvailable.RequestSenderToAll senderToAll =
				new RpcStrategyAllAvailable.RequestSenderToAll(asList(sender1, sender2, sender3));
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
		RpcRequestSender sender1 = new RequestSenderOnResultWithNullCaller();
		RpcRequestSender sender2 = new RequestSenderOnResultWithNullCaller();
		RpcRequestSender sender3 = new RequestSenderOnResultWithValueCaller();
		RpcStrategyAllAvailable.RequestSenderToAll senderToAll =
				new RpcStrategyAllAvailable.RequestSenderToAll(asList(sender1, sender2, sender3));
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
		RpcRequestSendingStrategy strategy = new RpcStrategyAllAvailable(null);
	}

	static final class RequestSenderOnResultWithNullCaller implements RpcRequestSender {

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, ResultCallback<T> callback) {
			callback.onResult(null);
		}
	}

	static final class RequestSenderOnResultWithValueCaller implements RpcRequestSender {

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, ResultCallback<T> callback) {
			callback.onResult((T) new RpcMessageDataStub());
		}
	}
}
