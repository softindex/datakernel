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
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderRendezvousHashing extends RequestSenderToGroup {
	private static final int HASH_BASE = 104;
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();

	private final HashFunction<RpcMessage.RpcMessageData> hashFunction;
	@VisibleForTesting
	final HashBucket hashBucket;


	public RequestSenderRendezvousHashing(List<RequestSender> senders, HashFunction<RpcMessage.RpcMessageData> hashFunction) {
		super(senders);
		this.hashFunction = checkNotNull(hashFunction);
		this.hashBucket = new HashBucket(senders);
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
	                                                              final ResultCallback<T> callback) {
		checkNotNull(callback);
		RequestSender sender = getRequestSender(request);
		if (sender == null) {
			callback.onException(NO_AVAILABLE_CONNECTION);
			return;
		}
		sender.sendRequest(request, timeout, callback);
	}

	private RequestSender getRequestSender(RpcMessage.RpcMessageData request) {
		int hash = hashFunction.hashCode(request);
		return hashBucket.chooseSender(hash);
	}

	@Override
	protected int getHashBase() {
		return HASH_BASE;
	}

	@VisibleForTesting
	static class HashBucket {
		private static final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
		private static final com.google.common.hash.HashFunction murmurHashAddressFunction = Hashing.murmur3_32();

		private final byte[] baseHashes;

		// TODO: maybe use same senders as in base class ?
		private final List<RequestSender> senders;

		public HashBucket(List<RequestSender> senders) {
			this(senders, DEFAULT_BUCKET_CAPACITY);
		}

		public HashBucket(List<RequestSender> senders, int capacity) {
			checkArgument((capacity & (capacity - 1)) == 0, "capacity must be a power-of-two, got %d", capacity);
			this.senders = checkNotNull(senders, "addresses is not set");
			this.baseHashes = new byte[capacity];
			computeBaseHashes();
		}

		// if activeAddresses is empty fill bucket -1
		private void computeBaseHashes() {
			for (int n = 0; n < baseHashes.length; n++) {
				int senderIndex = -1;
				int max = Integer.MIN_VALUE;
				for (int i = 0; i < senders.size(); ++i) {
					RequestSender sender = senders.get(i);
					if (sender.isActive()) {
						int hash = hashAddress(n, sender);
						if (hash >= max) {
							senderIndex = i;
							max = hash;
						}
					}
				}
				// TODO (vmykhalko): We can't use more than 128 senders in list. Is it desirable/acceptable limitation?
				baseHashes[n] = (byte) senderIndex;
			}
		}

		private int hashAddress(int bucket, RequestSender sender) {
			return murmurHashAddressFunction.newHasher()
					.putInt(sender.getKey())
					.putInt(bucket)
					.hash().asInt();
		}

		public RequestSender chooseSender(int hash) {
			int index = baseHashes[hash & (baseHashes.length - 1)];
			return senders.get(index);
		}
	}
}
