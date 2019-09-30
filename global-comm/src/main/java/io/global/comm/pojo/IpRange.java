package io.global.comm.pojo;

import java.net.InetAddress;
import java.util.Arrays;

public final class IpRange {
	private final byte[] ip;
	private final byte[] mask;

	public IpRange(byte[] ip, byte[] mask) {
		this.ip = ip;
		this.mask = mask;
	}

	public byte[] getIp() {
		return ip;
	}

	public byte[] getMask() {
		return mask;
	}

	public boolean test(InetAddress address) {
		byte[] raw = address.getAddress();
		if (raw.length != 4) {
			return false;
		}
		for (int i = 0; i < 4; i++) {
			if ((ip[i] & mask[i]) != (raw[i] & mask[i])) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		IpRange range = (IpRange) o;

		return Arrays.equals(ip, range.ip) && Arrays.equals(mask, range.mask);
	}

	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(ip) + Arrays.hashCode(mask);
	}
}
