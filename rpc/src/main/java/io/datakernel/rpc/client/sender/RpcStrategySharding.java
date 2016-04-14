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
import io.datakernel.rpc.hash.ShardingFunction;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategySharding implements RpcStrategy {
	private final RpcStrategyList list;
	private final ShardingFunction<?> shardingFunction;
	private int minActiveSubStrategies;

	public RpcStrategySharding(ShardingFunction<?> shardingFunction, RpcStrategyList list) {
		this.shardingFunction = checkNotNull(shardingFunction);
		this.list = list;
	}

	public RpcStrategySharding withMinActiveSubStrategies(int minActiveSubStrategies) {
		this.minActiveSubStrategies = minActiveSubStrategies;
		return this;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return list.getAddresses();
	}

	@Override
	public final RpcSender createSender(RpcClientConnectionPool pool) {
		List<RpcSender> subSenders = list.listOfNullableSenders(pool);
		int activeSenders = 0;
		for (RpcSender subSender : subSenders) {
			if (subSender != null) {
				activeSenders++;
			}
		}
		if (activeSenders < minActiveSubStrategies)
			return null;
		if (subSenders.size() == 0)
			return null;
		if (subSenders.size() == 1)
			return subSenders.get(0);
		return new Sender(shardingFunction, subSenders);
	}

	static final class Sender implements RpcSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderException("No senders available");

		private final ShardingFunction<?> shardingFunction;
		private final RpcSender[] subSenders;

		public Sender(ShardingFunction<?> shardingFunction, List<RpcSender> senders) {
			// null values are allowed in senders list
			checkArgument(senders != null && senders.size() > 0);
			this.shardingFunction = checkNotNull(shardingFunction);
			this.subSenders = senders.toArray(new RpcSender[senders.size()]);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			checkNotNull(callback);

			int shardIndex = ((ShardingFunction<Object>) shardingFunction).getShard(request);
			RpcSender sender = subSenders[shardIndex];
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
			} else {
				callback.fireException(NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}

	}
}
