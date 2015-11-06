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

import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import java.net.InetSocketAddress;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class RpcRequestSendingStrategies {

	private RpcRequestSendingStrategies() {

	}

	public static RpcStrategySingleServer server(final InetSocketAddress address) {
		checkNotNull(address);
		return new RpcStrategySingleServer(address);
	}

	public static RpcStrategyServersGroup servers(InetSocketAddress... addresses) {
		return servers(asList(addresses));
	}

	public static RpcStrategyServersGroup servers(final List<InetSocketAddress> addresses) {
		checkNotNull(addresses);
		checkArgument(addresses.size() > 0, "at least one address must be present");
		return new RpcStrategyServersGroup(addresses);
	}

	public static RpcStrategyFirstAvailable firstAvailable(RpcRequestSendingStrategy... senders) {
		return firstAvailable(asList(senders));
	}

	public static RpcStrategyFirstAvailable firstAvailable(final List<RpcRequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RpcStrategyFirstAvailable(senders);
	}

	public static RpcStrategyAllAvailable allAvailable(RpcRequestSendingStrategy... senders) {
		return allAvailable(asList(senders));
	}

	public static RpcStrategyAllAvailable allAvailable(final List<RpcRequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RpcStrategyAllAvailable(senders);
	}

	public static RpcStrategyRoundRobin roundRobin(RpcRequestSendingStrategy... senders) {
		return roundRobin(asList(senders));
	}

	public static RpcStrategyRoundRobin roundRobin(final List<RpcRequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RpcStrategyRoundRobin(senders);
	}

	public static RpcStrategySharding sharding(final HashFunction<RpcMessageData> hashFunction,
	                                        RpcRequestSendingStrategy... senders) {
		return sharding(hashFunction, asList(senders));
	}

	public static RpcStrategySharding sharding(final HashFunction<RpcMessageData> hashFunction,
	                                        final List<RpcRequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return new RpcStrategySharding(hashFunction, senders);
	}

	public static RpcStrategyRendezvousHashing rendezvousHashing(final HashFunction<RpcMessageData> hashFunction) {
		checkNotNull(hashFunction);
		return new RpcStrategyRendezvousHashing(hashFunction);
	}

	public static RpcStrategyTypeDispatching typeDispatching() {
		return new RpcStrategyTypeDispatching();
	}
}

