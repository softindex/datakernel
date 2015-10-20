///*
// * Copyright (C) 2015 SoftIndex LLC.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.datakernel.rpc.hash;
//
//import com.google.common.collect.Lists;
//import com.google.common.net.InetAddresses;
//import io.datakernel.bytebuf.ByteBufPool;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.net.InetSocketAddress;
//import java.util.List;
//
//import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
// TODO (vmykhalko): update tests
//
//public class HashBucketAddressesTest {
//	@Before
//	public void before() {
//		ByteBufPool.clear();
//		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
//	}
//
//	@Test
//	public void testHashBucketAddress() {
//		final int COUNT_SERVERS = 5;
//		final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
//		final List<InetSocketAddress> addresses = buildInetSocketAddressListIp(COUNT_SERVERS);
//		final HashBucketAddresses hashBucket = new HashBucketAddresses(addresses, DEFAULT_BUCKET_CAPACITY);
//
//		List<InetSocketAddress> activeAddresses = Lists.newArrayList(addresses);
//		hashBucket.updateBucket(activeAddresses);
//		byte[] baseBucket = new byte[DEFAULT_BUCKET_CAPACITY];
//
//		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++)
//			baseBucket[i] = (byte) hashBucket.getAddressId(i); //
//
//		activeAddresses.remove(1);
//		hashBucket.updateBucket(activeAddresses);
//		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
//			if (baseBucket[i] != 1)
//				assertEquals(baseBucket[i], hashBucket.getAddressId(i));
//			else
//				assertTrue(hashBucket.getAddressId(i) != 1);
//		}
//
//		activeAddresses.remove(1); // 2
//		hashBucket.updateBucket(activeAddresses);
//		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
//			if (baseBucket[i] != 1 && baseBucket[i] != 2) {
//				assertEquals(baseBucket[i], hashBucket.getAddressId(i));
//			} else {
//				assertTrue(hashBucket.getAddressId(i) != 1);
//				assertTrue(hashBucket.getAddressId(i) != 2);
//			}
//
//		}
//
//		activeAddresses.add(addresses.get(1));
//		hashBucket.updateBucket(activeAddresses);
//		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
//			if (baseBucket[i] != 2)
//				assertEquals(baseBucket[i], hashBucket.getAddressId(i));
//			else
//				assertTrue(hashBucket.getAddressId(i) != 2);
//		}
//
//		activeAddresses.add(addresses.get(2));
//		hashBucket.updateBucket(activeAddresses);
//		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
//			assertEquals(baseBucket[i], hashBucket.getAddressId(i));
//		}
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
//	}
//
//	static List<InetSocketAddress> buildInetSocketAddressListIp(int COUNT_SERVERS) {
//		List<InetSocketAddress> addresses = Lists.newArrayList();
//		for (int i = 0; i < COUNT_SERVERS; ++i) {
//			InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("192.168.64." + (3 + i)), 5555);
//			addresses.add(address);
//		}
//		return addresses;
//	}
//}

