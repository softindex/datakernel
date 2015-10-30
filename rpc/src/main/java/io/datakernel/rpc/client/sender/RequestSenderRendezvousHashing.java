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
import com.google.common.hash.Hashing;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.hash.BucketHashFunction;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderRendezvousHashing implements RequestSender {

	private static final RpcNoSenderAvailableException NO_ACTIVE_SENDER_AVAILABLE_EXCEPTION
			= new RpcNoSenderAvailableException("No active senders available");
	private static final BucketHashFunction DEFAULT_BUCKET_HASH_FUNCTION = new DefaultBucketHashFunction();

	private final boolean active;
	private final HashFunction<RpcMessage.RpcMessageData> hashFunction;
	@VisibleForTesting
	final RendezvousHashBucket<RequestSender> hashBucket;


	public RequestSenderRendezvousHashing(Map<Object, RequestSender> keyToSender,
	                                      HashFunction<RpcMessage.RpcMessageData> hashFunction,
	                                      BucketHashFunction bucketHashFunction) {
		checkNotNull(keyToSender);
		this.hashFunction = checkNotNull(hashFunction);
		this.active = countActiveSenders(keyToSender) > 0;
		this.hashBucket = new RendezvousHashBucket(keyToSender, bucketHashFunction);
	}

	public RequestSenderRendezvousHashing(Map<Object, RequestSender> keyToSender,
	                                      HashFunction<RpcMessage.RpcMessageData> hashFunction) {
		this(keyToSender, hashFunction, DEFAULT_BUCKET_HASH_FUNCTION);
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
	                                                              final ResultCallback<T> callback) {
		checkNotNull(callback);

		assert isActive();

		RequestSender sender = getRequestSender(request);
		if (sender == null) {
			callback.onException(NO_ACTIVE_SENDER_AVAILABLE_EXCEPTION);
			return;
		}
		sender.sendRequest(request, timeout, callback);
	}

	@Override
	public final boolean isActive() {
		return active;
	}

	private RequestSender getRequestSender(RpcMessage.RpcMessageData request) {
		int hash = hashFunction.hashCode(request);
		return hashBucket.chooseSender(hash);
	}

	private static int countActiveSenders(Map<Object, RequestSender> keyToSender) {
		int count = 0;
		for (RequestSender sender : keyToSender.values()) {
			if (sender.isActive()) {
				++count;
			}
		}
		return count;
	}

	@VisibleForTesting
	static final class RendezvousHashBucket<T> {
		private static final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
		private static final com.google.common.hash.HashFunction murmurHashAddressFunction = Hashing.murmur3_32();

		private final Object[] baseHashes;
		private final BucketHashFunction bucketHashFunction;
		private final Map<Object, RequestSender> keyToSender;

		public RendezvousHashBucket(Map<Object, RequestSender> keyToSender, BucketHashFunction bucketHashFunction) {
			this(keyToSender, bucketHashFunction, DEFAULT_BUCKET_CAPACITY);
		}

		public RendezvousHashBucket(Map<Object, RequestSender> keyToSender,
		                            BucketHashFunction bucketHashFunction, int capacity) {
			checkArgument((capacity & (capacity - 1)) == 0, "capacity must be a power-of-two, got %d", capacity);
			checkNotNull(bucketHashFunction);
			this.keyToSender = checkNotNull(keyToSender);
			this.baseHashes = new Object[capacity];
			this.bucketHashFunction = bucketHashFunction;
			computeBaseHashes();
		}

		// if activeAddresses is empty fill bucket -1
		private void computeBaseHashes() {
			for (int n = 0; n < baseHashes.length; n++) {
				Object senderKey = null;
				int max = Integer.MIN_VALUE;
				for (Object key : keyToSender.keySet()) {
					RequestSender sender = keyToSender.get(key);
					if (sender.isActive()) {
						int hash = bucketHashFunction.hash(key, n);
						if (hash >= max) {
							senderKey = key;
							max = hash;
						}
					}
				}
				baseHashes[n] = senderKey;
			}
		}

		public RequestSender chooseSender(int hash) {
			Object key = baseHashes[hash & (baseHashes.length - 1)];
			if (key != null) {
				return keyToSender.get(key);
			} else {
				return null;
			}
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
