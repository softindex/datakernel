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

package io.datakernel.rpc.hash;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HashBucketAddresses {
	private static final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
	private static final HashFunction murmurHashAddressFunction = Hashing.murmur3_32();

	private final byte[] baseHashes;
	private final List<InetSocketAddress> addresses;

	public HashBucketAddresses(List<InetSocketAddress> addresses) {
		this(addresses, DEFAULT_BUCKET_CAPACITY);
	}

	public HashBucketAddresses(List<InetSocketAddress> addresses, int capacity) {
		checkArgument((capacity & (capacity - 1)) == 0, "capacity must be a power-of-two, got %d", capacity);
		this.addresses = checkNotNull(addresses, "addresses is not set");
		this.baseHashes = new byte[capacity];
	}

	// if activeAddresses is empty fill bucket -1
	public void updateBucket(Collection<InetSocketAddress> activeAddresses) {
		for (int n = 0; n < baseHashes.length; n++) {
			int addressId = -1;
			int max = Integer.MIN_VALUE;
			for (int i = 0; i < addresses.size(); i++) {
				InetSocketAddress address = addresses.get(i);

				if (!activeAddresses.contains(address))
					continue;

				int hash = hashAddress(n, address);
				if (hash >= max) {
					addressId = i;
					max = hash;
				}
			}
			baseHashes[n] = (byte) addressId;
		}
	}

	private static int ipv4ToInt(InetAddress address) {
		byte[] ipAddressBytes = address.getAddress();
		int result = ipAddressBytes[0] & 0xff;
		result |= (ipAddressBytes[1] << 8) & 0xff00;
		result |= (ipAddressBytes[2] << 16) & 0xff0000;
		result |= (ipAddressBytes[3] << 24) & 0xff000000;
		return result;
	}

	private int hashAddress(int bucket, InetSocketAddress address) {
		return murmurHashAddressFunction.newHasher()
				.putInt(ipv4ToInt(address.getAddress()))
				.putInt(address.getPort())
				.putInt(bucket)
				.hash().asInt();
	}

	public int getAddressId(int hash) {
		return baseHashes[hash & (baseHashes.length - 1)];
	}
}
