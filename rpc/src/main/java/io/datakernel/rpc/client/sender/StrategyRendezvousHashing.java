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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.hash.Hashing;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.BucketHashFunction;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

public final class StrategyRendezvousHashing extends AbstractRequestSendingStrategy implements SingleSenderStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;
	private static final BucketHashFunction DEFAULT_BUCKET_HASH_FUNCTION = new DefaultBucketHashFunction();

	private final int minSubStrategiesForCreation;
	private final Map<Object, SingleSenderStrategy> keyToStrategy;
	private final HashFunction<RpcMessage.RpcMessageData> hashFunction;
	private final BucketHashFunction bucketHashFunction;

	StrategyRendezvousHashing(HashFunction<RpcMessage.RpcMessageData> hashFunction) {
		this(hashFunction, DEFAULT_BUCKET_HASH_FUNCTION);
	}

	StrategyRendezvousHashing(HashFunction<RpcMessage.RpcMessageData> hashFunction,
	                          BucketHashFunction bucketHashFunction) {
		this(hashFunction, bucketHashFunction, MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT);
	}

	StrategyRendezvousHashing(HashFunction<RpcMessage.RpcMessageData> hashFunction,
	                          int minSubStrategiesForCreation) {
		this(hashFunction, DEFAULT_BUCKET_HASH_FUNCTION, minSubStrategiesForCreation);
	}

	StrategyRendezvousHashing(HashFunction<RpcMessage.RpcMessageData> hashFunction,
	                          BucketHashFunction bucketHashFunction,
	                          int minSubStrategiesForCreation) {
		this.keyToStrategy = new HashMap<>();
		this.hashFunction = checkNotNull(hashFunction);
		this.bucketHashFunction = checkNotNull(bucketHashFunction);
		this.minSubStrategiesForCreation = minSubStrategiesForCreation;
	}


	public StrategyRendezvousHashing put(Object key, SingleSenderStrategy strategy) {
		checkNotNull(strategy);
		keyToStrategy.put(key, strategy);
		return this;
	}



	@Override
	protected List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public Optional<RequestSender> create(RpcClientConnectionPool pool) {
		Map<Object, Optional<RequestSender>> keyToSender = createKeyToSender(pool, keyToStrategy);
		if (countPresentValues(keyToSender) >= minSubStrategiesForCreation) {
			return Optional.<RequestSender>of(
					new RequestSenderRendezvousHashing(keyToSender, hashFunction, bucketHashFunction));
		} else {
			return Optional.absent();
		}
	}

	private static Map<Object, Optional<RequestSender>> createKeyToSender(RpcClientConnectionPool pool,
			Map<Object, SingleSenderStrategy> keyToStrategy) {

		assert keyToStrategy != null;

		Map<Object, Optional<RequestSender>> keyToSender = new HashMap<>();
		for (Object key : keyToStrategy.keySet()) {
			SingleSenderStrategy strategy = keyToStrategy.get(key);
			Optional<RequestSender> sender = strategy.create(pool);
			keyToSender.put(key, sender);
		}
		return keyToSender;
	}

	private static <T> int countPresentValues(Map<Object, Optional<T>> keyToSender) {
		int counter = 0;
		for (Optional<T> value : keyToSender.values()) {
			if (value.isPresent()) {
				++counter;
			}
		}
		return counter;
	}


	@VisibleForTesting
	final static class RequestSenderRendezvousHashing implements RequestSender {

		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No active senders available");

		private final HashFunction<RpcMessage.RpcMessageData> hashFunction;
		final RendezvousHashBucket<RequestSender> hashBucket;


		public RequestSenderRendezvousHashing(Map<Object, Optional<RequestSender>> keyToSender,
		                                      HashFunction<RpcMessage.RpcMessageData> hashFunction,
		                                      BucketHashFunction bucketHashFunction) {
			checkNotNull(keyToSender);
			this.hashFunction = checkNotNull(hashFunction);
			this.hashBucket = new RendezvousHashBucket(keyToSender, bucketHashFunction);
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              final ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender sender = getRequestSender(request);
			if (sender == null) {
				callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
				return;
			}
			sender.sendRequest(request, timeout, callback);
		}

		private RequestSender getRequestSender(RpcMessage.RpcMessageData request) {
			int hash = hashFunction.hashCode(request);
			return hashBucket.chooseSender(hash);
		}
	}

	@VisibleForTesting
	static final class RendezvousHashBucket<T> {
		private static final int DEFAULT_BUCKET_CAPACITY = 1 << 11;

		private final RequestSender[] sendersBucket;

		public RendezvousHashBucket(Map<Object, Optional<RequestSender>> keyToSender,
		                            BucketHashFunction bucketHashFunction) {
			this(keyToSender, bucketHashFunction, DEFAULT_BUCKET_CAPACITY);
		}

		public RendezvousHashBucket(Map<Object, Optional<RequestSender>> keyToSender,
		                            BucketHashFunction bucketHashFunction, int capacity) {
			checkArgument((capacity & (capacity - 1)) == 0, "capacity must be a power-of-two, got %d", capacity);
			checkNotNull(bucketHashFunction);
			this.sendersBucket = new RequestSender[capacity];
			computeBaseHashes(keyToSender, bucketHashFunction);
		}

		// if activeAddresses is empty fill bucket with null
		private void computeBaseHashes(Map<Object, Optional<RequestSender>> keyToSender,
		                               BucketHashFunction bucketHashFunction) {
			for (int n = 0; n < sendersBucket.length; n++) {
				RequestSender chosenSender = null;
				int max = Integer.MIN_VALUE;
				for (Object key : keyToSender.keySet()) {
					Optional<RequestSender> sender = keyToSender.get(key);
					if (sender.isPresent()) {
						int hash = bucketHashFunction.hash(key, n);
						if (hash >= max) {
							chosenSender = sender.get();
							max = hash;
						}
					}
				}
				sendersBucket[n] = chosenSender;
			}
		}

		public RequestSender chooseSender(int hash) {
			return sendersBucket[hash & (sendersBucket.length - 1)];
		}
	}

	@VisibleForTesting
	static final class DefaultBucketHashFunction implements BucketHashFunction {
		private static final com.google.common.hash.HashFunction murmurHashAddressFunction = Hashing.murmur3_32();

		@Override
		public int hash(Object key, int bucket) {
			return murmurHashAddressFunction.newHasher()
					.putInt(key.hashCode())
					.putInt(bucket)
					.hash().asInt();
		}
	}
}
