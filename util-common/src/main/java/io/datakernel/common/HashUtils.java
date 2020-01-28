package io.datakernel.common;

public final class HashUtils {

	public static long murmur3hash(long k) {
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;
		return k;
	}

	public static int murmur3hash(int firstHash, int secondHash) {
		return (int) murmur3hash(((long) firstHash << 32) | (secondHash & 0xFFFFFFFFL));
	}

	public static int murmur3hash(int k) {
		return (int) murmur3hash((long) k);
	}

}
