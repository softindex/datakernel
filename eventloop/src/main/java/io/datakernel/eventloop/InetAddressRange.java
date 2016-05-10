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

package io.datakernel.eventloop;

import io.datakernel.async.ParseException;
import io.datakernel.util.Preconditions;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

@SuppressWarnings("unused, WeakerAccess")
public final class InetAddressRange implements Comparable<InetAddressRange>, Iterable<InetAddress> {
	private static final int[] EMPTY_MASK = {0, 0, 0, 0};
	public static final int[] START = new int[]{0, 0, 0, 0};
	public static final int[] END = new int[]{-1, -1, -1, -1};

	/*
	*   Represents address in binary form:
	*
	*   128 bits == 4 int for ipv6
	*   32  bits == last 4th int in array for ipv4
	*   using BE notation
	* */

	private final int network[];
	private final int mask[];
	private final int end[];

	private boolean acceptAll;

	// creators & accessors
	private InetAddressRange(int[] network, int[] mask, int[] end) {
		this.network = network;
		this.mask = mask;
		this.end = end;
	}

	private InetAddressRange(int[] start, int[] end) {
		this(start, EMPTY_MASK, end);
	}

	private InetAddressRange() {
		this(START, END);
		acceptAll = true;
	}

	/**
	 * Supported formats:<br/>
	 * All:         *
	 * CIDR:        address/cidrPrefix : 192.168.1.0/24<br/>
	 * RANGE:       address1-address2  : 192.168.1.0 - 192.169.1.255<br/>
	 * ALONE:       address            : 192.168.0.1
	 * <br/>
	 * Not Supported:<br/>
	 * RANGE/CIDR   address1-address2/cidrPrefix
	 */
	public static InetAddressRange parse(String s) throws ParseException {
		// *
		if (s.equals("*")) {
			return new InetAddressRange();
		}

		// a.b.c.d/cidr
		int index = s.indexOf('/');
		if (index >= 0) {
			String address = s.substring(0, index);
			try {
				int cidrPrefix = Integer.parseInt(s.substring(index + 1));
				InetAddress inetAddress = forString(address);
				if (inetAddress instanceof Inet4Address) {
					return fromCidr((Inet4Address) inetAddress, cidrPrefix);
				} else {
					return fromCidr((Inet6Address) inetAddress, cidrPrefix);
				}
			} catch (NumberFormatException e) {
				throw new ParseException("Invalid cidrPrefix");
			}
		}

		// a.b.c.d - e.f.g.j
		index = s.indexOf('-');
		if (index >= 0) {
			InetAddress address1 = forString(s.substring(0, index));
			InetAddress address2 = forString(s.substring(index + 1));
			if (address1 instanceof Inet4Address && address2 instanceof Inet4Address) {
				return fromRange((Inet4Address) address1, (Inet4Address) address2);
			} else if (address1 instanceof Inet6Address && address2 instanceof Inet6Address) {
				return fromRange((Inet6Address) address1, (Inet6Address) address2);
			} else {
				throw new ParseException("Invalid inet addresses range. You should specify either ipv4 or ipv6");
			}
		}

		// a.b.c.d
		return fromAddress(forString(s));
	}

	public static InetAddressRange all() {
		return new InetAddressRange();
	}

	public static InetAddressRange fromAddressMask(Inet6Address address, Inet6Address mask) throws ParseException {
		return fromAddressMask(ip2int(address), ip2int(mask));
	}

	public static InetAddressRange fromAddressMask(Inet4Address address, Inet4Address mask) throws ParseException {
		return fromAddressMask(ip2int(address), ip2int(mask));
	}

	private static InetAddressRange fromAddressMask(int[] address, int[] mask) throws ParseException {
		if (!isValidMask(mask)) {
			throw new ParseException("Invalid mask: " + forInteger(mask).toString());
		}
		int[] network = new int[4];
		int[] end = new int[4];

		for (int i = 0; i < 4; i++) {
			network[i] = address[i] & mask[i];
			end[i] = network[i] | ~mask[i];
		}

		return new InetAddressRange(network, mask, end);
	}

	public static InetAddressRange fromCidr(Inet4Address address, int cidrPrefix) throws ParseException {
		return fromCidr(ip2int(address), cidrPrefix + 96);
	}

	public static InetAddressRange fromCidr(Inet6Address address, int cidrPrefix) throws ParseException {
		return fromCidr(ip2int(address), cidrPrefix);
	}

	private static InetAddressRange fromCidr(int[] address, int cidrPrefix) throws ParseException {
		return fromAddressMask(address, cidrPrefix2mask(cidrPrefix));
	}

	public static InetAddressRange fromRange(Inet4Address start, Inet4Address end) throws ParseException {
		return fromRange(ip2int(start), ip2int(end));
	}

	public static InetAddressRange fromRange(Inet6Address start, Inet6Address end) throws ParseException {
		return fromRange(ip2int(start), ip2int(end));
	}

	private static InetAddressRange fromRange(int[] start, int[] end) throws ParseException {
		if (compareBitsArrays(start, end) > 0)
			throw new IllegalArgumentException("Invalid range: " + forInteger(start) + " > " + forInteger(end));
		int[] mask = range2mask(start, end);
		if (isValidMask(mask)) {
			InetAddressRange inetAddressRange = fromAddressMask(start, mask);
			if (start == inetAddressRange.network && end == inetAddressRange.end)
				return inetAddressRange;
		}
		return new InetAddressRange(start, end);
	}

	public static InetAddressRange fromAddress(InetAddress address) {
		return fromAddress(ip2int(address));
	}

	private static InetAddressRange fromAddress(int[] address) {
		return new InetAddressRange(address, address);
	}

	public InetAddress getStartAddress() {
		try {
			return forInteger(network);
		} catch (ParseException e) {
			throw new IllegalStateException("Start address is not valid", e);
		}
	}

	public InetAddress getEndAddress() {
		try {
			return forInteger(end);
		} catch (ParseException e) {
			throw new IllegalStateException("End address is not valid", e);
		}
	}

	public InetAddress getMaskAddress() {
		Preconditions.check(isSubnet(), "IpRange is not subnet");
		try {
			return forInteger(mask);
		} catch (ParseException e) {
			throw new IllegalStateException("Mask is not valid");
		}
	}

	public InetAddress getNetworkAddress() {
		try {
			Preconditions.check(isSubnet(), "IpRange is not subnet");
			return forInteger(network);
		} catch (ParseException e) {
			throw new IllegalStateException("network address is not valid");
		}
	}

	public int getCidrPrefix() {
		Preconditions.check(isSubnet(), "IpRange is not subnet");
		return pop(mask);
	}

	// api
	public boolean contains(InetAddress address) {
		return contains(ip2int(address));
	}

	private boolean contains(int[] address) {
		return acceptAll || (compareBitsArrays(address, network) >= 0 && compareBitsArrays(address, end) <= 0);
	}

	public boolean isSubnet() {
		return !Arrays.equals(mask, EMPTY_MASK);
	}

	public boolean isSingle() {
		return Arrays.equals(network, end);
	}

	// static utils
	private static InetAddress forString(String s) throws ParseException {
		try {
			return InetAddress.getByName(s);
		} catch (UnknownHostException e) {
			throw new ParseException("Invalid network supplied", e);
		}
	}

	private static InetAddress forInteger(int[] network) throws ParseException {
		try {
			return InetAddress.getByAddress(toByteArray(network));
		} catch (UnknownHostException e) {
			throw new ParseException("Invalid network supplied", e);
		}
	}

	private static int[] cidrPrefix2mask(int cidrPrefix) throws ParseException {
		if (cidrPrefix <= 0 || cidrPrefix > 128) {
			throw new ParseException("Invalid cidrPrefix " + cidrPrefix);
		}
		int pos = 0;
		int[] mask = new int[4];
		while (cidrPrefix > 32) {
			mask[pos++] = -1;
			cidrPrefix -= 32;
		}
		for (int j = 0; j < cidrPrefix; ++j) {
			mask[pos] |= (1 << 31 - j);
		}
		return mask;
	}

	private static int[] ip2int(InetAddress address) {
		if (address instanceof Inet4Address) {
			return new int[]{0, 0, 0, address.hashCode()};
		} else {
			return fromByteArray(address.getAddress());
		}
	}

	private static int[] range2mask(int[] low, int[] high) {
		int[] mask = new int[4];
		for (int i = 0; i < 4; i++) {
			mask[i] = ~(low[i] ^ high[i]);
		}
		return mask;
	}

	private static boolean isValidMask(int[] mask) {
		int[] m = new int[4];
		for (int i = 0; i < 4; i++) {
			m[i] = ~mask[i];
		}
		mask = inc(m);
		for (int i = 0; i < 4; i++) {
			if ((m[i] & mask[i]) != 0) {
				return false;
			}
		}
		return true;
	}

	private static int[] inc(int[] arr) {
		int[] res = new int[arr.length];
		int val = 1;
		for (int i = arr.length - 1; i >= 0; i--) {
			if (val != 0 && arr[i] == -1) {
				res[i] = 0;
			} else if (val != 0) {
				res[i] = arr[i] + val;
				val = 0;
			} else {
				res[i] = arr[i];
			}
		}
		return res;
	}

	private static int compareBitsArrays(int[] start, int[] end) {
		int res;
		for (int i = 0; i < 4; i++) {
			for (int j = 31; j >= 0; j--) {
				int mask = 1 << j;
				if ((start[i] & mask) != (end[i] & mask)) {
					if ((start[i] & mask) >> j == 1) {
						return 1;
					} else {
						return -1;
					}
				}
			}
		}
		return 0;
	}

	private static int pop(int[] arr) {
		int num = 0;
		for (int x : arr) {
			num += pop(x);
		}
		return num;
	}

	/*
	* Transforms mask to cidr
	* Count the number of 1-bits in a 32-bit integer using a divide-and-conquer strategy
    * see Hacker's Delight section 5.1
	*/
	private static int pop(int x) {
		x = x - ((x >>> 1) & 0x55555555);
		x = (x & 0x33333333) + ((x >>> 2) & 0x33333333);
		x = (x + (x >>> 4)) & 0x0F0F0F0F;
		x = x + (x >>> 8);
		x = x + (x >>> 16);
		return x & 0x0000003F;
	}

	private static int[] fromByteArray(byte[] address) {
		int[] ints = new int[4];
		for (int i = 0; i < 4; i++) {
			ints[i] = (address[i * 4] & 0xFF) << 24;
			ints[i] |= (address[i * 4 + 1] & 0xFF) << 16;
			ints[i] |= (address[i * 4 + 2] & 0xFF) << 8;
			ints[i] |= (address[i * 4 + 3] & 0xFF);
		}
		return ints;
	}

	private static byte[] toByteArray(int[] ints) {
		byte[] res = new byte[16];
		for (int i = 0; i < ints.length; i++) {
			toByteArray(res, 4 * i, ints[i]);
		}
		return res;
	}

	private static void toByteArray(byte[] bytes, int pos, int val) {
		bytes[pos] = (byte) ((val & 0xFF000000) >> 24);
		bytes[pos + 1] = (byte) ((val & 0x00FF0000) >> 16);
		bytes[pos + 2] = (byte) ((val & 0x0000FF00) >> 8);
		bytes[pos + 3] = (byte) (val & 0x000000FF);
	}

	@Override
	public String toString() {
		if (isSingle()) {
			return getStartAddress().toString();
		} else {
			return isSubnet() ? toCidrString() : toRangeString();
		}
	}

	public String toCidrString() {
		return getNetworkAddress().toString() + "/" + pop(mask);
	}

	public String toRangeString() {
		return getStartAddress().toString() + "-" + getEndAddress().toString();
	}

	@Override
	public int hashCode() {
		int hash = 31;
		for (int i = 0; i < network.length; i++) {
			hash += (31 * network[i]) + end[i];
		}
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;
		InetAddressRange o = (InetAddressRange) obj;
		return Arrays.equals(network, o.network) && Arrays.equals(end, o.end);
	}

	@Override
	public int compareTo(InetAddressRange o) {
		int c = compareBitsArrays(network, o.network);
		if (c != 0) {
			return c;
		}
		return compareBitsArrays(end, o.end);
	}

	private static class InetAddressIterator implements Iterator<InetAddress> {
		private int[] current;
		private final int[] end;

		InetAddressIterator(int[] start, int[] end) {
			this.current = start;
			this.end = inc(end);
		}

		@Override
		public boolean hasNext() {
			return compareBitsArrays(current, end) < 0;
		}

		@Override
		public InetAddress next() throws NoSuchElementException {
			if (compareBitsArrays(current, end) == 0) {
				throw new NoSuchElementException();
			}
			try {
				return InetAddress.getByAddress(toByteArray(current));
			} catch (UnknownHostException e) {
				throw new AssertionError("Should not ever get here");
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Remove is not supported");
		}
	}

	@Override
	public Iterator<InetAddress> iterator() {
		return new InetAddressIterator(network, end);
	}
}
