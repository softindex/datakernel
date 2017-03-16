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

import io.datakernel.exception.SimpleException;

/**
 * Represents an exception which stores name of domain with which error was created,
 * end error code.
 */
public final class DnsException extends SimpleException {
	private final String domainName;
	private final DnsMessage.ResponseErrorCode errorCode;

	/**
	 * Creates a new instance of DnsException
	 *
	 * @param domainName domain name with which error was created
	 * @param errorCode  ResponseErrorCode for this domain name
	 */
	public DnsException(String domainName, DnsMessage.ResponseErrorCode errorCode) {
		super("DNS query for domain " + domainName + " failed with error code: " + errorCode.name());
		this.domainName = domainName;
		this.errorCode = errorCode;
	}

	public DnsMessage.ResponseErrorCode getErrorCode() {
		return errorCode;
	}

	public String getDomainName() {
		return domainName;
	}
}
