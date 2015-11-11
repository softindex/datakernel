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

import com.google.common.base.Optional;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.HashFunction;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.rpc.client.sender.RpcSendersUtils.flatten;
import static io.datakernel.rpc.client.sender.RpcSendersUtils.replaceAbsentToNull;
import static java.util.Arrays.asList;

public final class RpcStrategySharding implements RpcRequestSendingStrategy, RpcSingleSenderStrategy {
	private final List<RpcRequestSendingStrategy> subStrategies;
	private final HashFunction<Object> hashFunction;

	public RpcStrategySharding(HashFunction<Object> hashFunction, List<RpcRequestSendingStrategy> subStrategies) {
		this.hashFunction = checkNotNull(hashFunction);
		this.subStrategies = checkNotNull(subStrategies);
	}

	@Override
	public final List<Optional<RpcRequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public final Optional<RpcRequestSender> create(RpcClientConnectionPool pool) {
		List<Optional<RpcRequestSender>> subSenders = createSubSenders(pool);
		return Optional.<RpcRequestSender>of(new RequestSenderSharding(hashFunction, replaceAbsentToNull(subSenders)));
	}

	private final List<Optional<RpcRequestSender>> createSubSenders(RpcClientConnectionPool pool) {

		assert subStrategies != null;

		List<List<Optional<RpcRequestSender>>> listOfListOfSenders = new ArrayList<>();
		for (RpcRequestSendingStrategy subStrategy : subStrategies) {
			listOfListOfSenders.add(subStrategy.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}

	final static class RequestSenderSharding implements RpcRequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No senders available");
		private final HashFunction<Object> hashFunction;
		private final RpcRequestSender[] subSenders;

		public RequestSenderSharding(HashFunction<Object> hashFunction, List<RpcRequestSender> senders) {
			// null values are allowed in senders list
			checkArgument(senders != null && senders.size() > 0);
			this.hashFunction = checkNotNull(hashFunction);
			this.subSenders = senders.toArray(new RpcRequestSender[senders.size()]);
		}

		@Override
		public <T> void sendRequest(Object request, int timeout, final ResultCallback<T> callback) {
			checkNotNull(callback);

			RpcRequestSender sender = chooseSender(request);
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
				return;
			}
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}

		private RpcRequestSender chooseSender(Object request) {
			int index = Math.abs(hashFunction.hashCode(request)) % subSenders.length;
			return subSenders[index];
		}
	}
}
