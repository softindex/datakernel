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
import io.datakernel.rpc.hash.HashBucketFunction;
import io.datakernel.rpc.hash.HashFunction;

import java.net.InetSocketAddress;
import java.util.*;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategyRendezvousHashing implements RpcStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;
	private static final int DEFAULT_BUCKET_CAPACITY = 2048;
	private static final HashBucketFunction DEFAULT_BUCKET_HASH_FUNCTION = new DefaultHashBucketFunction();

	private final Map<Object, RpcStrategy> shards = new HashMap<>();
	private final HashFunction<?> hashFunction;
	private int minShards = MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT;
	private HashBucketFunction hashBucketFunction = DEFAULT_BUCKET_HASH_FUNCTION;
	private int buckets = DEFAULT_BUCKET_CAPACITY;

	public RpcStrategyRendezvousHashing(HashFunction<?> hashFunction) {
		this.hashFunction = checkNotNull(hashFunction);
	}

	public RpcStrategyRendezvousHashing withMinActiveShards(int minShards) {
		checkArgument(minShards > 0, "minSubStrategiesForCreation must be greater than 0");
		this.minShards = minShards;
		return this;
	}

	public RpcStrategyRendezvousHashing withHashBucketFunction(HashBucketFunction hashBucketFunction) {
		this.hashBucketFunction = checkNotNull(hashBucketFunction);
		return this;
	}

	public RpcStrategyRendezvousHashing withHashBuckets(int buckets) {
		checkArgument((buckets & (buckets - 1)) == 0, "Buckets number must be a power-of-two, got %d", buckets);
		this.buckets = buckets;
		return this;
	}

	public RpcStrategyRendezvousHashing put(Object shardId, RpcStrategy strategy) {
		checkNotNull(strategy);
		shards.put(shardId, strategy);
		return this;
	}

	public RpcStrategyRendezvousHashing putAddresses(InetSocketAddress... addresses) {
		putAddresses(Arrays.asList(addresses));
		return this;
	}

	public RpcStrategyRendezvousHashing putAddresses(List<InetSocketAddress> addresses) {
		for (InetSocketAddress address : addresses) {
			shards.put(address, new RpcStrategySingleServer(address));
		}
		return this;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcStrategy strategy : shards.values()) {
			result.addAll(strategy.getAddresses());
		}
		return result;
	}

	@Override
	public RpcSender createSender(RpcClientConnectionPool pool) {
		Map<Object, RpcSender> shardsSenders = new HashMap<>();
		for (Map.Entry<Object, RpcStrategy> entry : shards.entrySet()) {
			Object shardId = entry.getKey();
			RpcStrategy strategy = entry.getValue();
			RpcSender sender = strategy.createSender(pool);
			if (sender != null) {
				shardsSenders.put(shardId, sender);
			}
		}
		if (shardsSenders.size() < minShards)
			return null;
		if (shardsSenders.size() == 1) {
			return shardsSenders.values().iterator().next();
		}

		checkNotNull(hashBucketFunction);
		RpcSender[] sendersBuckets = new RpcSender[buckets];
		for (int n = 0; n < sendersBuckets.length; n++) {
			RpcSender chosenSender = null;
			int max = Integer.MIN_VALUE;
			for (Map.Entry<Object, RpcSender> entry : shardsSenders.entrySet()) {
				Object key = entry.getKey();
				RpcSender sender = entry.getValue();
				int hash = hashBucketFunction.hash(key, n);
				if (hash >= max) {
					chosenSender = sender;
					max = hash;
				}
			}
			assert chosenSender != null;
			sendersBuckets[n] = chosenSender;
		}
		return new Sender(hashFunction, sendersBuckets);
	}

	static final class Sender implements RpcSender {
		private final HashFunction<?> hashFunction;
		private final RpcSender[] hashBuckets;

		public Sender(HashFunction<?> hashFunction, RpcSender[] hashBuckets) {
			this.hashFunction = checkNotNull(hashFunction);
			this.hashBuckets = checkNotNull(hashBuckets);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			RpcSender sender = chooseBucket(request);
			sender.sendRequest(request, timeout, callback);
		}

		@SuppressWarnings("unchecked")
		RpcSender chooseBucket(Object request) {
			int hash = ((HashFunction<Object>) hashFunction).hashCode(request);
			return hashBuckets[hash & (hashBuckets.length - 1)];
		}
	}

	// visible for testing
	static final class DefaultHashBucketFunction implements HashBucketFunction {
		@Override
		public int hash(Object shardId, int bucket) {
			int shardIdHash = shardId.hashCode();
			long k = (((long) shardIdHash) << 32) | (bucket & 0xFFFFFFFFL);
			k ^= k >>> 33;
			k *= 0xff51afd7ed558ccdL;
			k ^= k >>> 33;
			k *= 0xc4ceb9fe1a85ec53L;
			k ^= k >>> 33;
			return (int) k;
		}
	}
}
