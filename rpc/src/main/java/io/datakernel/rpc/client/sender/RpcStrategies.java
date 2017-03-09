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
import io.datakernel.rpc.hash.ShardingFunction;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

/**
 * Strategies used in RPC can be divided in three following categories:
 * <ul>
 *     <li>Getting a single RPC-service</li>
 *     <li>Failover</li>
 *     <li>Load balancing</li>
 * </ul>
 */
public final class RpcStrategies {
	private RpcStrategies() {
	}

	public static RpcStrategySingleServer server(InetSocketAddress address) {
		checkNotNull(address);
		return RpcStrategySingleServer.create(address);
	}

	public static RpcStrategyList servers(InetSocketAddress... addresses) {
		return servers(asList(addresses));
	}

	public static RpcStrategyList servers(List<InetSocketAddress> addresses) {
		return RpcStrategyList.ofAddresses(addresses);
	}

	public static RpcStrategyFirstAvailable firstAvailable(RpcStrategy... senders) {
		return firstAvailable(asList(senders));
	}

	public static RpcStrategyFirstAvailable firstAvailable(List<RpcStrategy> strategies) {
		return RpcStrategyFirstAvailable.create(RpcStrategyList.ofStrategies(strategies));
	}

	public static RpcStrategyFirstAvailable firstAvailable(RpcStrategyList list) {
		return RpcStrategyFirstAvailable.create(list);
	}

	public static RpcStrategyFirstValidResult firstValidResult(RpcStrategy... senders) {
		return firstValidResult(asList(senders));
	}

	public static RpcStrategyFirstValidResult firstValidResult(List<RpcStrategy> senders) {
		return RpcStrategyFirstValidResult.create(RpcStrategyList.ofStrategies(senders));
	}

	public static RpcStrategyFirstValidResult firstValidResult(RpcStrategyList list) {
		return RpcStrategyFirstValidResult.create(list);
	}

	public static RpcStrategyRoundRobin roundRobin(RpcStrategy... senders) {
		return roundRobin(asList(senders));
	}

	public static RpcStrategyRoundRobin roundRobin(List<RpcStrategy> senders) {
		return RpcStrategyRoundRobin.create(RpcStrategyList.ofStrategies(senders));
	}

	public static RpcStrategyRoundRobin roundRobin(RpcStrategyList list) {
		return RpcStrategyRoundRobin.create(list);
	}

	public static RpcStrategySharding sharding(final ShardingFunction<?> hashFunction,
	                                           RpcStrategy... senders) {
		return sharding(hashFunction, asList(senders));
	}

	public static RpcStrategySharding sharding(final ShardingFunction<?> hashFunction,
	                                           final List<RpcStrategy> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return RpcStrategySharding.create(hashFunction, RpcStrategyList.ofStrategies(senders));
	}

	public static RpcStrategySharding sharding(final ShardingFunction<?> hashFunction,
	                                           RpcStrategyList list) {
		checkNotNull(list);
		checkNotNull(hashFunction);
		return RpcStrategySharding.create(hashFunction, list);
	}

	public static RpcStrategyRendezvousHashing rendezvousHashing(final HashFunction<?> hashFunction) {
		checkNotNull(hashFunction);
		return RpcStrategyRendezvousHashing.create(hashFunction);
	}

	public static RpcStrategyTypeDispatching typeDispatching() {
		return RpcStrategyTypeDispatching.create();
	}

	public static RpcStrategyRandomSampling randomSampling() {
		return RpcStrategyRandomSampling.create();
	}
}

