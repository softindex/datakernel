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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * SimpleDnsResolver is realization of the {@link BlockingDnsResolver}. For resolving domain it is used
 * java.net.* .
 */
public final class SimpleDnsResolver extends BlockingDnsResolver {
	public SimpleDnsResolver(Executor executor) {
		super(executor);
	}

	@Override
	protected ResolvedAddress doResolve(String host, boolean ipv6) {
		ResolvedAddress resolvedAddress;
		try {
			InetAddress[] addresses = InetAddress.getAllByName(host);

			// remove unsolicited addresses
			addresses = filter(addresses, ipv6);

			resolvedAddress = new ResolvedAddress(addresses, positiveKeepMillis);
		} catch (Exception e) {
			if (logger.isWarnEnabled()) {
				logger.warn("Failed to resolve address by host name {}: {}", host, e);
			}
			return new ResolvedAddress(null, negativeKeepMillis);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Resolved addresses: {} for host: {}", resolvedAddress.getAllAddresses(), host);
		}
		return resolvedAddress;
	}

	private InetAddress[] filter(InetAddress[] unfiltered, final boolean ipv6) {
		Collection<InetAddress> filtered = new ArrayList<>();
		for (InetAddress inetAddress : unfiltered) {
			if (!ipv6 && inetAddress instanceof Inet4Address) {
				filtered.add(inetAddress);
			} else if (ipv6 && inetAddress instanceof Inet6Address) {
				filtered.add(inetAddress);
			}
		}
		return filtered.toArray(new InetAddress[filtered.size()]);
	}
}
