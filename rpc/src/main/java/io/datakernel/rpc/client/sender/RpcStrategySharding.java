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
import io.datakernel.rpc.hash.Sharder;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.rpc.client.sender.RpcSendersUtils.flatten;
import static io.datakernel.rpc.client.sender.RpcSendersUtils.replaceAbsentToNull;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class RpcStrategySharding implements RpcRequestSendingStrategy, RpcSingleSenderStrategy {
	private final List<RpcRequestSendingStrategy> subStrategies;
	private final Sharder<Object> sharder;

	public RpcStrategySharding(Sharder<Object> sharder, List<RpcRequestSendingStrategy> subStrategies) {
		this.sharder = checkNotNull(sharder);
		this.subStrategies = checkNotNull(subStrategies);
	}

	@Override
	public final List<RpcRequestSenderHolder> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public final RpcRequestSenderHolder create(RpcClientConnectionPool pool) {
		List<RpcRequestSenderHolder> subSenders = createSubSenders(pool);
		return RpcRequestSenderHolder.of(new RequestSenderSharding(sharder, replaceAbsentToNull(subSenders)));
	}

	private final List<RpcRequestSenderHolder> createSubSenders(RpcClientConnectionPool pool) {

		assert subStrategies != null;

		List<List<RpcRequestSenderHolder>> listOfListOfSenders = new ArrayList<>();
		for (RpcRequestSendingStrategy subStrategy : subStrategies) {
			listOfListOfSenders.add(subStrategy.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}

	final static class RequestSenderSharding implements RpcRequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No senders available");
		private final Sharder<Object> sharder;
		private final RpcRequestSender[] subSenders;

		public RequestSenderSharding(Sharder<Object> sharder, List<RpcRequestSender> senders) {
			// null values are allowed in senders list
			checkArgument(senders != null && senders.size() > 0);
			this.sharder = checkNotNull(sharder);
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
			int shardIndex = sharder.getShard(request);
			return subSenders[shardIndex];
		}
	}
}
