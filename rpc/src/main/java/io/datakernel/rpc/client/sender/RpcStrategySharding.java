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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategySharding implements RpcRequestSendingStrategy {
	private final RpcStrategyList list;
	private final Sharder<?> sharder;

	public RpcStrategySharding(Sharder<?> sharder, RpcStrategyList list) {
		this.sharder = checkNotNull(sharder);
		this.list = list;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return list.getAddresses();
	}

	@Override
	public final RpcRequestSender createSender(RpcClientConnectionPool pool) {
		List<RpcRequestSender> subSenders = list.listOfNullableSenders(pool);
		return new RequestSenderSharding(sharder, subSenders);
	}

	final static class RequestSenderSharding implements RpcRequestSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No senders available");

		private final Sharder<?> sharder;
		private final RpcRequestSender[] subSenders;

		public RequestSenderSharding(Sharder<?> sharder, List<RpcRequestSender> senders) {
			// null values are allowed in senders list
			checkArgument(senders != null && senders.size() > 0);
			this.sharder = checkNotNull(sharder);
			this.subSenders = senders.toArray(new RpcRequestSender[senders.size()]);
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			checkNotNull(callback);

			RpcRequestSender sender = chooseSender(request);
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
				return;
			}
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}

		private RpcRequestSender chooseSender(Object request) {
			int shardIndex = ((Sharder<Object>) sharder).getShard(request);
			return subSenders[shardIndex];
		}
	}
}
