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

import io.datakernel.async.callback.Callback;
import io.datakernel.common.HashUtils;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.HashBucketFunction;
import io.datakernel.rpc.hash.HashFunction;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.common.collection.CollectionUtils.first;

public final class RpcStrategyRendezvousHashing implements RpcStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;
	private static final int DEFAULT_BUCKET_CAPACITY = 2048;
	private static final HashBucketFunction DEFAULT_BUCKET_HASH_FUNCTION = new DefaultHashBucketFunction();

	private final Map<Object, RpcStrategy> shards;
	private final HashFunction<?> hashFunction;
	private final int minShards;
	private final HashBucketFunction hashBucketFunction;
	private final int buckets;

	private RpcStrategyRendezvousHashing(HashFunction<?> hashFunction, int minShards,
										 HashBucketFunction hashBucketFunction, int buckets,
										 Map<Object, RpcStrategy> shards) {
		this.hashFunction = checkNotNull(hashFunction);
		this.minShards = minShards;
		this.hashBucketFunction = hashBucketFunction;
		this.buckets = buckets;
		this.shards = shards;
	}

	public static RpcStrategyRendezvousHashing create(HashFunction<?> hashFunction) {
		return new RpcStrategyRendezvousHashing(hashFunction, MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT,
				DEFAULT_BUCKET_HASH_FUNCTION, DEFAULT_BUCKET_CAPACITY, new HashMap<>());
	}

	public RpcStrategyRendezvousHashing withMinActiveShards(int minShards) {
		checkArgument(minShards > 0, "minSubStrategiesForCreation must be greater than 0");
		return new RpcStrategyRendezvousHashing(hashFunction, minShards, hashBucketFunction, buckets, shards);
	}

	public RpcStrategyRendezvousHashing withHashBucketFunction(HashBucketFunction hashBucketFunction) {
		return new RpcStrategyRendezvousHashing(hashFunction, minShards, hashBucketFunction, buckets, shards);
	}

	public RpcStrategyRendezvousHashing withHashBuckets(int buckets) {
		checkArgument((buckets & (buckets - 1)) == 0, "Buckets number must be a power-of-two, got %d", buckets);
		return new RpcStrategyRendezvousHashing(hashFunction, minShards, hashBucketFunction, buckets, shards);
	}

	public RpcStrategyRendezvousHashing withShard(Object shardId, RpcStrategy strategy) {
		checkNotNull(strategy);
		shards.put(shardId, strategy);
		return this;
	}

	public RpcStrategyRendezvousHashing withShards(InetSocketAddress... addresses) {
		return withShards(Arrays.asList(addresses));
	}

	public RpcStrategyRendezvousHashing withShards(List<InetSocketAddress> addresses) {
		for (InetSocketAddress address : addresses) {
			shards.put(address, RpcStrategySingleServer.create(address));
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
	@Nullable
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
		if (shardsSenders.size() < minShards) {
			return null;
		}
		if (shardsSenders.size() == 1) {
			return first(shardsSenders.values());
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

		Sender(HashFunction<?> hashFunction, RpcSender[] hashBuckets) {
			this.hashFunction = checkNotNull(hashFunction);
			this.hashBuckets = checkNotNull(hashBuckets);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			int hash = ((HashFunction<Object>) hashFunction).hashCode(request);
			RpcSender sender = hashBuckets[hash & (hashBuckets.length - 1)];
			sender.sendRequest(request, timeout, cb);
		}

	}

	// visible for testing
	static final class DefaultHashBucketFunction implements HashBucketFunction {
		@Override
		public int hash(Object shardId, int bucket) {
			return HashUtils.murmur3hash(shardId.hashCode(), bucket);
		}
	}
}
