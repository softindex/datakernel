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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.ResultCallback;
import io.datakernel.dns.DnsCache.DnsCacheQueryResult;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

import static io.datakernel.dns.DnsCache.DnsCacheQueryResult.*;
import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static io.datakernel.util.Preconditions.checkArgument;

/**
 * NativeDnsResolver represents asynchronous DNS resolver, which run in Eventloop.
 */
public final class NativeDnsResolver implements DnsClient, NativeDnsResolverMBean {
	private final Logger logger = LoggerFactory.getLogger(NativeDnsResolver.class);

	public static final DatagramSocketSettings DEFAULT_DATAGRAM_SOCKET_SETTINGS = defaultDatagramSocketSettings();

	private final Eventloop eventloop;

	private DnsClientConnection connection;
	private final DatagramSocketSettings datagramSocketSettings;

	private InetSocketAddress dnsServerAddress;

	private final DnsCache cache;

	private final long timeout;

	private static final int DNS_SERVER_PORT = 53;
	private static final long ONE_MINUTE_MILLIS = 60 * 1000L;

	/**
	 * Creates a new NativeDnsResolver
	 *
	 * @param eventloop        eventloop in which it will be ran
	 * @param timeout          time which this resolver will wait result
	 * @param dnsServerAddress address of DNS server which will resolve domain names
	 */
	public NativeDnsResolver(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout, InetSocketAddress dnsServerAddress) {
		this(eventloop, datagramSocketSettings, timeout, dnsServerAddress,
				ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS);
	}

	/**
	 * Creates a new NativeDnsResolver
	 *
	 * @param eventloop        eventloop in which it will be ran
	 * @param timeout          time which this resolver will wait result
	 * @param dnsServerAddress address of DNS server which will resolve domain names
	 */
	public NativeDnsResolver(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout, InetAddress dnsServerAddress) {
		this(eventloop, datagramSocketSettings, timeout, new InetSocketAddress(dnsServerAddress, DNS_SERVER_PORT),
				ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS);
	}

	public NativeDnsResolver(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout, InetSocketAddress dnsServerAddress,
	                         long errorCacheExpirationMillis, long hardExpirationDeltaMillis) {
		this.eventloop = eventloop;
		this.datagramSocketSettings = datagramSocketSettings;
		this.timeout = timeout;
		this.dnsServerAddress = dnsServerAddress;
		this.cache = new DnsCache(eventloop, errorCacheExpirationMillis, hardExpirationDeltaMillis);
	}

	/**
	 * Returns the DNS client which will run in other eventloop
	 *
	 * @param eventloop eventloop in which DnsClient will be ran
	 * @return DNS client which will run in other eventloop
	 */
	public DnsClient getDnsClientForAnotherEventloop(final Eventloop eventloop) {
		if (eventloop == this.eventloop)
			return this;

		return new DnsClient() {
			@Override
			public void resolve4(final String domainName, final ResultCallback<InetAddress[]> callback) {
				resolve(domainName, false, callback);
			}

			@Override
			public void resolve6(final String domainName, final ResultCallback<InetAddress[]> callback) {
				resolve(domainName, true, callback);
			}

			private void resolve(final String domainName, final boolean ipv6, final ResultCallback<InetAddress[]> callback) {
				checkArgument(domainName != null, "Domain name must not be null");

				if (Utils.isInetAddress(domainName)) {
					callback.onResult(new InetAddress[]{Utils.forString(domainName)});
					return;
				}

				DnsCacheQueryResult cacheQueryResult = cache.tryToResolve(domainName, ipv6, callback);

				if (cacheQueryResult == RESOLVED)
					return;

				if (cacheQueryResult == RESOLVED_NEEDS_REFRESHING) {
					NativeDnsResolver.this.eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							NativeDnsResolver.this.resolve(domainName, ipv6, AsyncCallbacks.<InetAddress[]>ignoreResultCallback());
						}
					});
					return;
				}

				if (cacheQueryResult == NOT_RESOLVED) {
					NativeDnsResolver.this.eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							NativeDnsResolver.this.resolve(domainName, ipv6, AsyncCallbacks.concurrentResultCallback(eventloop, callback));
						}
					});
				}
			}
		};
	}

	/**
	 * Resolves the IP for the IPv4 addresses and handles it with callback
	 *
	 * @param domainName domain name for searching IP
	 * @param callback   result callback
	 */
	@Override
	public void resolve4(final String domainName, ResultCallback<InetAddress[]> callback) {
		resolve(domainName, false, callback);
	}

	/**
	 * Resolves the IP for the IPv6 addresses and handles it with callback
	 *
	 * @param domainName domain name for searching IP
	 * @param callback   result callback
	 */
	@Override
	public void resolve6(String domainName, ResultCallback<InetAddress[]> callback) {
		resolve(domainName, true, callback);
	}

	private void resolve(final String domainName, final boolean ipv6, final ResultCallback<InetAddress[]> callback) {
		checkArgument(domainName != null, "Domain name must not be null");

		if (Utils.isInetAddress(domainName)) {
			callback.onResult(new InetAddress[]{Utils.forString(domainName)});
			return;
		}

		DnsCacheQueryResult cacheQueryResult = cache.tryToResolve(domainName, ipv6, callback);

		if (cacheQueryResult == RESOLVED) {
			cache.performCleanup();
			return;
		}

		final boolean resolvedFromCache = cacheQueryResult == RESOLVED_NEEDS_REFRESHING;

		logger.trace("Resolving {} with DNS server.", domainName);

		final ResultCallback<DnsQueryResult> queryCachingCallback = new ResultCallback<DnsQueryResult>() {
			@Override
			public void onResult(DnsQueryResult result) {
				if (callback != null && !resolvedFromCache) {
					callback.onResult(result.getIps());
				}
				cache.add(result);
				closeConnectionIfDone();
			}

			@Override
			public void onException(Exception exception) {
				if (exception instanceof DnsException) {
					DnsException dnsException = (DnsException) exception;
					cache.add(dnsException);
				}
				if (callback != null && !resolvedFromCache) {
					callback.onException(exception);
				}
				closeConnectionIfDone();
			}

			private void closeConnectionIfDone() {
				if (connection != null && connection.allRequestsCompleted()) {
					connection.close();
					connection = null;
				}
			}
		};

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				if (connection == null || !connection.isRegistered()) {
					registerConnection();
				}
				assert connection != null;
				if (ipv6) {
					connection.resolve6(domainName, dnsServerAddress, timeout, queryCachingCallback);
				} else {
					connection.resolve4(domainName, dnsServerAddress, timeout, queryCachingCallback);
				}
			}
		});

		cache.performCleanup();
	}

	/**
	 * Registers a new DnsConnection in its eventloop.
	 */
	public void registerConnection() {
		try {
			DatagramChannel datagramChannel = createDatagramChannel(datagramSocketSettings, null, dnsServerAddress);
			connection = new DnsClientConnection(eventloop, datagramChannel);
			connection.register();
		} catch (IOException e) {
			if (logger.isErrorEnabled())
				logger.error("DnsClientConnection registration failed.", e);
		}
	}

	public Eventloop getEventloop() {
		return eventloop;
	}

	DnsCache getCache() {
		return cache;
	}

	@Override
	public int getNumberOfCachedDomainNames() {
		return cache.getNumberOfCachedDomainNames();
	}

	@Override
	public int getNumberOfCachedExceptions() {
		return cache.getNumberOfCachedExceptions();
	}

	@Override
	public int getNumberOfQueriesInProgress() {
		if (connection == null) {
			return 0;
		} else {
			return connection.getNumberOfRequestsInProgress();
		}
	}

	@Override
	public String[] getDomainNamesBeingResolved() {
		if (connection == null) {
			return new String[0];
		} else {
			return connection.getDomainNamesBeingResolved();
		}
	}

	@Override
	public String[] getAllCacheEntries() {
		return cache.getAllCacheEntries();
	}

	@Override
	public String[] getSuccessfullyResolvedDomainNames() {
		return cache.getSuccessfullyResolvedDomainNames();
	}

	@Override
	public String[] getDomainNamesOfFailedRequests() {
		return cache.getDomainNamesOfFailedRequests();
	}

	@Override
	public void emptyCache() {
		cache.emptyCache();
	}

	@Override
	public void setMaxTtlMillis(long maxTtlMillis) {
		cache.setMaxTtlMillis(maxTtlMillis);
	}

	@Override
	public long getMaxTtlMillis() {
		return cache.getMaxTtlMillis();
	}
}
