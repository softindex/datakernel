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

import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategyList {
	private final List<RpcStrategy> strategies;

	private RpcStrategyList(List<RpcStrategy> strategies) {
		this.strategies = strategies;
	}

	public static RpcStrategyList ofAddresses(List<InetSocketAddress> addresses) {
		checkNotNull(addresses);
		checkArgument(addresses.size() > 0, "at least one address must be present");
		List<RpcStrategy> subSenders = new ArrayList<>();
		for (InetSocketAddress address : addresses) {
			subSenders.add(new RpcStrategySingleServer(address));
		}
		return new RpcStrategyList(subSenders);
	}

	public static RpcStrategyList ofStrategies(List<RpcStrategy> strategies) {
		return new RpcStrategyList(new ArrayList<>(strategies));
	}

	public List<RpcSender> listOfSenders(RpcClientConnectionPool pool) {
		List<RpcSender> senders = new ArrayList<>();
		for (RpcStrategy strategy : strategies) {
			RpcSender sender = strategy.createSender(pool);
			if (sender != null) {
				senders.add(sender);
			}
		}
		return senders;
	}

	public List<RpcSender> listOfNullableSenders(RpcClientConnectionPool pool) {
		List<RpcSender> senders = new ArrayList<>();
		for (RpcStrategy strategy : strategies) {
			RpcSender sender = strategy.createSender(pool);
			senders.add(sender);
		}
		return senders;
	}

	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcStrategy sender : strategies) {
			result.addAll(sender.getAddresses());
		}
		return result;
	}

	public int size() {
		return strategies.size();
	}

	public RpcStrategy get(int index) {
		return strategies.get(index);
	}

}
