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
import static io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;
import static java.util.Arrays.asList;

public final class StrategySharding extends AbstractRequestSendingStrategy implements SingleSenderStrategy {
	private final List<RequestSendingStrategy> subStrategies;
	private final HashFunction<RpcMessageData> hashFunction;

	public StrategySharding(HashFunction<RpcMessageData> hashFunction, List<RequestSendingStrategy> subStrategies) {
		this.hashFunction = checkNotNull(hashFunction);
		this.subStrategies = checkNotNull(subStrategies);
	}

	@Override
	protected final List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public final Optional<RequestSender> create(RpcClientConnectionPool pool) {
		List<Optional<RequestSender>> subSenders = createSubSenders(pool);
		return Optional.<RequestSender>of(new RequestSenderSharding(hashFunction, replaceAbsentToNull(subSenders)));
	}

	private final List<Optional<RequestSender>> createSubSenders(RpcClientConnectionPool pool) {

		assert subStrategies != null;

		List<List<Optional<RequestSender>>> listOfListOfSenders = new ArrayList<>();
		for (RequestSendingStrategy subStrategy : subStrategies) {
			AbstractRequestSendingStrategy abstractSubSendingStrategy = (AbstractRequestSendingStrategy) subStrategy;
			listOfListOfSenders.add(abstractSubSendingStrategy.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}

	final static class RequestSenderSharding implements RequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No senders available");
		private final HashFunction<RpcMessageData> hashFunction;
		private final RequestSender[] subSenders;

		public RequestSenderSharding(HashFunction<RpcMessageData> hashFunction, List<RequestSender> senders) {
			// null values are allowed in senders list
			checkArgument(senders != null && senders.size() > 0);
			this.hashFunction = checkNotNull(hashFunction);
			this.subSenders = senders.toArray(new RequestSender[senders.size()]);
		}

		@Override
		public <T extends RpcMessageData> void sendRequest(RpcMessageData request, int timeout,
		                                                   final ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender sender = chooseSender(request);
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
				return;
			}
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}

		private RequestSender chooseSender(RpcMessageData request) {
			int index = Math.abs(hashFunction.hashCode(request)) % subSenders.length;
			return subSenders[index];
		}
	}
}
