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

package io.datakernel.dns;

import java.net.InetAddress;

/**
 * Represent a resolved domain
 */
public final class DnsResourceRecord {
	private final InetAddress[] ips;
	private final int minTtl;
	private final short type;

	/**
	 * Creates a new instance of DnsResourceRecord
	 *
	 * @param ips    address of the resolved domain
	 * @param minTtl time to live for this record
	 * @param type   type of address
	 */
	private DnsResourceRecord(InetAddress[] ips, int minTtl, short type) {
		this.ips = ips;
		this.minTtl = minTtl;
		this.type = type;
	}

	public static DnsResourceRecord of(InetAddress[] ips, int minTtl, short type) {
		return new DnsResourceRecord(ips, minTtl, type);
	}

	public boolean hasData() {
		return ips.length != 0;
	}

	public InetAddress[] getIps() {
		return ips;
	}

	public int getMinTtl() {
		return minTtl;
	}

	public short getType() {
		return type;
	}
}
