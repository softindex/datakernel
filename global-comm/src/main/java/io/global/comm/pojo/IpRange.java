package io.global.comm.pojo;

import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;

public final class IpRange implements Comparable<IpRange> {
	private long lowerBound;
	private long upperBound;

	public IpRange(InetAddress lowerBound, InetAddress upperBound) {
		this.lowerBound = ipToLong(lowerBound);
		this.upperBound = ipToLong(upperBound);
	}

	public IpRange(long lowerBound, long upperBound) {
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}

	public long getLowerBound() {
		return lowerBound;
	}

	public long getUpperBound() {
		return upperBound;
	}

	public boolean test(InetAddress address) {
		long x = ipToLong(address);
		return x <= upperBound && x >= lowerBound;
	}

	private static long ipToLong(InetAddress ip) {
		byte[] octets = ip.getAddress();
		long result = 0;
		for (byte octet : octets) {
			result <<= 8;
			result |= octet & 0xff;
		}
		return result;
	}

	@Override
	public int compareTo(@NotNull IpRange o) {
		int result = Long.compare(lowerBound, upperBound);
		return result != 0 ? result : Long.compare(upperBound, upperBound);
	}
}
