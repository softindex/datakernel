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

public final class RequestSendingStrategies {

	private RequestSendingStrategies() {

	}

	public static StrategySingleServer server(final InetSocketAddress address) {
		checkNotNull(address);
		return new StrategySingleServer(address);
	}

	public static StrategyServersGroup servers(InetSocketAddress... addresses) {
		return servers(asList(addresses));
	}

	public static StrategyServersGroup servers(final List<InetSocketAddress> addresses) {
		checkNotNull(addresses);
		checkArgument(addresses.size() > 0, "at least one address must be present");
		return new StrategyServersGroup(addresses);
	}

	public static StrategyFirstAvailable firstAvailable(RequestSendingStrategy... senders) {
		return firstAvailable(asList(senders));
	}

	public static StrategyFirstAvailable firstAvailable(final List<RequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new StrategyFirstAvailable(senders);
	}

	public static StrategyAllAvailable allAvailable(RequestSendingStrategy... senders) {
		return allAvailable(asList(senders));
	}

	public static StrategyAllAvailable allAvailable(final List<RequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new StrategyAllAvailable(senders);
	}

	public static StrategyRoundRobin roundRobin(RequestSendingStrategy... senders) {
		return roundRobin(asList(senders));
	}

	public static StrategyRoundRobin roundRobin(final List<RequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new StrategyRoundRobin(senders);
	}

	public static StrategySharding sharding(final HashFunction<RpcMessageData> hashFunction,
	                                        RequestSendingStrategy... senders) {
		return sharding(hashFunction, asList(senders));
	}

	public static StrategySharding sharding(final HashFunction<RpcMessageData> hashFunction,
	                                        final List<RequestSendingStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return new StrategySharding(hashFunction, senders);
	}

	public static StrategyRendezvousHashing rendezvousHashing(final HashFunction<RpcMessageData> hashFunction) {
		checkNotNull(hashFunction);
		return new StrategyRendezvousHashing(hashFunction);
	}

	public static StrategyTypeDispatching typeDispatching() {
		return new StrategyTypeDispatching();
	}
}

