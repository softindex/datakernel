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

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.datakernel.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static io.datakernel.rpc.client.sender.RpcStrategies.firstValidResult;
import static io.datakernel.rpc.client.sender.RpcStrategies.servers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ConstantConditions")
public class RpcStrategyFirstValidResultTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldSendRequestToAllAvailableSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy firstValidResult = firstValidResult(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		int callsAmountIterationOne = 10;
		int callsAmountIterationTwo = 25;
		RpcSender senderToAll;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		senderToAll = firstValidResult.createSender(pool);
		for (int i = 0; i < callsAmountIterationOne; i++) {
			senderToAll.sendRequest(new Object(), 50);
		}
		pool.remove(ADDRESS_1);
		// we should recreate sender after changing in pool
		senderToAll = firstValidResult.createSender(pool);
		for (int i = 0; i < callsAmountIterationTwo; i++) {
			senderToAll.sendRequest(new Object(), 50);
		}

		assertEquals(callsAmountIterationOne, connection1.getRequests());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection2.getRequests());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection3.getRequests());
	}

	@Test
	public void itShouldCallOnResultWithNullIfAllSendersReturnedNullAndValidatorAndExceptionAreNotSpecified() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		RpcStrategy strategy1 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategy strategy2 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategy strategy3 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategyFirstValidResult firstValidResult = firstValidResult(strategy1, strategy2, strategy3);
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());

		final CompletableFuture<Object> future = sender.sendRequest(new Object(), 50).toCompletableFuture();

		// despite there are several sender, sendResult should be called only once after all senders returned null

		eventloop.run();
		assertEquals(null, future.get());
	}

	@Test(expected = Exception.class)
	public void itShouldCallOnExceptionIfAllSendersReturnsNullAndValidatorIsDefaultButExceptionIsSpecified() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		// default validator should check whether result is not null
		RpcStrategy strategy1 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategy strategy2 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategy strategy3 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategyFirstValidResult firstValidResult = firstValidResult(strategy1, strategy2, strategy3)
				.withNoValidResultException(new Exception());
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());

		final CompletableFuture<Object> future = sender.sendRequest(new Object(), 50).toCompletableFuture();

		eventloop.run();
		future.get();
	}

	@Test
	public void itShouldUseCustomValidatorIfItIsSpecified() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());

		final int invalidKey = 1;
		final int validKey = 2;
		RpcStrategy strategy1 = new RequestSenderOnResultWithValueStrategy(invalidKey);
		RpcStrategy strategy2 = new RequestSenderOnResultWithValueStrategy(validKey);
		RpcStrategy strategy3 = new RequestSenderOnResultWithValueStrategy(invalidKey);
		RpcStrategyFirstValidResult firstValidResult = firstValidResult(strategy1, strategy2, strategy3)
				.withResultValidator(new RpcStrategyFirstValidResult.ResultValidator<Integer>() {
					@Override
					public boolean isValidResult(Integer input) {
						return input == validKey;
					}
				})
				.withNoValidResultException(new Exception());
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());

		final CompletableFuture<Object> future = sender.sendRequest(new Object(), 50).toCompletableFuture();

		eventloop.run();
		assertEquals(validKey, future.get());
	}

	@Test(expected = Exception.class)
	public void itShouldCallOnExceptionIfNoSenderReturnsValidResultButExceptionWasSpecified() throws ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final int invalidKey = 1;
		final int validKey = 2;
		RpcStrategy strategy1 = new RequestSenderOnResultWithValueStrategy(invalidKey);
		RpcStrategy strategy2 = new RequestSenderOnResultWithValueStrategy(invalidKey);
		RpcStrategy strategy3 = new RequestSenderOnResultWithValueStrategy(invalidKey);
		RpcStrategyFirstValidResult firstValidResult = firstValidResult(strategy1, strategy2, strategy3)
				.withResultValidator(new RpcStrategyFirstValidResult.ResultValidator<Integer>() {
					@Override
					public boolean isValidResult(Integer input) {
						return input == validKey;
					}
				})
				.withNoValidResultException(new Exception());
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());
		final CompletableFuture<Object> future = sender.sendRequest(new Object(), 50).toCompletableFuture();
		eventloop.run();
		future.get();
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		// one connection is added
		pool.put(ADDRESS_2, connection);
		RpcStrategy firstValidResult = firstValidResult(servers(ADDRESS_1, ADDRESS_2));
		assertTrue(firstValidResult.createSender(pool) != null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategy firstValidResult = firstValidResult(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		assertTrue(firstValidResult.createSender(pool) == null);
	}

	private static final class SenderOnResultWithNullCaller implements RpcSender {
		@Override
		public <I, O> CompletionStage<O> sendRequest(I request, int timeout) {
			return SettableStage.immediateStage(null);
		}
	}

	static final class SenderOnResultWithValueCaller implements RpcSender {
		private final Object data;

		public SenderOnResultWithValueCaller(Object data) {
			this.data = data;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> CompletionStage<O> sendRequest(I request, int timeout) {
			return SettableStage.immediateStage((O) data);
		}
	}

	static final class RequestSenderOnResultWithNullStrategy implements RpcStrategy {
		@Override
		public Set<InetSocketAddress> getAddresses() {
			throw new UnsupportedOperationException();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return new SenderOnResultWithNullCaller();
		}
	}

	static final class RequestSenderOnResultWithValueStrategy implements RpcStrategy {
		private final Object data;

		public RequestSenderOnResultWithValueStrategy(Object data) {
			this.data = data;
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			throw new UnsupportedOperationException();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return new SenderOnResultWithValueCaller(data);
		}
	}
}
