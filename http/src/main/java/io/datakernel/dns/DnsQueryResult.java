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

import io.datakernel.dns.DnsMessage.ResponseErrorCode;

import java.net.InetAddress;

/**
 * A DNS query packet which is sent to a server to receive a DNS response packet with information
 * answering a DnsQuery's questions.
 */
public final class DnsQueryResult {
	private final String domainName;
	private final InetAddress[] ips;
	private final Integer minTtl;
	private final ResponseErrorCode errorCode;
	private final Short type;

	/**
	 * Returns the DnsQueryResult which has fail
	 *
	 * @param domainName result domain name
	 * @param errorCode  code of error
	 */
	public static DnsQueryResult failedQuery(String domainName, ResponseErrorCode errorCode) {
		return new DnsQueryResult(domainName, null, null, errorCode, null);
	}

	/**
	 * Returns the DnsQueryResult which has  gone successful
	 *
	 * @param domainName result domain name
	 * @param ips        result InetAddress
	 * @param minTtl     time to live this packet
	 * @param type       type of query packet
	 */
	public static DnsQueryResult successfulQuery(String domainName, InetAddress[] ips, Integer minTtl, Short type) {
		return new DnsQueryResult(domainName, ips, minTtl, ResponseErrorCode.NO_ERROR, type);
	}

	private DnsQueryResult(String domainName, InetAddress[] ips, Integer minTtl, ResponseErrorCode errorCode, Short type) {
		this.domainName = domainName;
		this.ips = ips;
		this.minTtl = minTtl;
		this.errorCode = errorCode;
		this.type = type;
	}

	public DnsException getException() {
		return new DnsException(domainName, errorCode);
	}

	public String getDomainName() {
		return domainName;
	}

	public InetAddress[] getIps() {
		return ips;
	}

	public Integer getMinTtl() {
		return minTtl;
	}

	public ResponseErrorCode getErrorCode() {
		return errorCode;
	}

	public boolean isSuccessful() {
		return errorCode == ResponseErrorCode.NO_ERROR;
	}

	public Short getType() {
		return type;
	}
}
