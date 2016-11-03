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

import io.datakernel.async.ConcurrentResultCallback;
import io.datakernel.async.IgnoreResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.dns.DnsCache.DnsCacheQueryResult;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.HttpUtils;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.net.DatagramSocketSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.dns.DnsCache.DnsCacheQueryResult.*;
import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;

public final class AsyncDnsClient implements IAsyncDnsClient, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(AsyncDnsClient.class);

	private static final int DNS_SERVER_PORT = 53;
	private static final long ONE_MINUTE_MILLIS = 60 * 1000L;
	public static final DatagramSocketSettings DEFAULT_DATAGRAM_SOCKET_SETTINGS = DatagramSocketSettings.create();
	public static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress(inetAddress("8.8.8.8"), DNS_SERVER_PORT);
	public static final InetSocketAddress LOCAL_DNS = new InetSocketAddress(inetAddress("192.168.0.1"), DNS_SERVER_PORT);
	public static final long DEFAULT_TIMEOUT = 3_000L;

	private final Eventloop eventloop;

	private DnsClientHandler connection;
	private final DatagramSocketSettings datagramSocketSettings;

	private InetSocketAddress dnsServerAddress;

	private final DnsCache cache;

	private final long timeout;

	// region builders
	public static AsyncDnsClient create(Eventloop eventloop) {
		DnsCache cache = DnsCache.create(eventloop, ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS);
		return new AsyncDnsClient(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
				DEFAULT_TIMEOUT, GOOGLE_PUBLIC_DNS, cache);
	}

	public AsyncDnsClient withDatagramSocketSetting(DatagramSocketSettings setting) {
		return new AsyncDnsClient(eventloop, setting, timeout, dnsServerAddress, cache);
	}

	/**
	 * @param timeout time which this resolver will wait result
	 */
	public AsyncDnsClient withTimeout(long timeout) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, dnsServerAddress, cache);
	}

	/**
	 * @param address address of DNS server which will resolve domain names
	 */
	public AsyncDnsClient withDnsServerAddress(InetSocketAddress address) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, address, cache);
	}

	/**
	 * @param address address of DNS server which will resolve domain names
	 */
	public AsyncDnsClient withDnsServerAddress(InetAddress address) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout,
				new InetSocketAddress(address, DNS_SERVER_PORT), cache);
	}

	public AsyncDnsClient withExpiration(long errorCacheExpirationMillis, long hardExpirationDeltaMillis) {
		DnsCache cache = DnsCache.create(eventloop, errorCacheExpirationMillis, hardExpirationDeltaMillis);
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, dnsServerAddress, cache);
	}
	// endregion

	private AsyncDnsClient(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout, InetSocketAddress dnsServerAddress,
	                       long errorCacheExpirationMillis, long hardExpirationDeltaMillis) {
		this.eventloop = eventloop;
		this.datagramSocketSettings = datagramSocketSettings;
		this.timeout = timeout;
		this.dnsServerAddress = dnsServerAddress;
		this.cache = DnsCache.create(eventloop, errorCacheExpirationMillis, hardExpirationDeltaMillis);
	}

	private AsyncDnsClient(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout, InetSocketAddress dnsServerAddress,
	                       DnsCache cache) {
		this.eventloop = eventloop;
		this.datagramSocketSettings = datagramSocketSettings;
		this.timeout = timeout;
		this.dnsServerAddress = dnsServerAddress;
		this.cache = cache;
	}

	/**
	 * Returns the DNS adapted client which will run in other eventloop using the same DNS cache
	 *
	 * @param eventloop eventloop in which DnsClient will be ran
	 * @return DNS client which will run in other eventloop
	 */
	public IAsyncDnsClient adaptToAnotherEventloop(final Eventloop eventloop) {
		if (eventloop == this.eventloop)
			return this;

		return new IAsyncDnsClient() {
			@Override
			public void resolve4(final String domainName, final ResultCallback<InetAddress[]> callback) {
				resolve(domainName, false, callback);
			}

			@Override
			public void resolve6(final String domainName, final ResultCallback<InetAddress[]> callback) {
				resolve(domainName, true, callback);
			}

			private void resolve(final String domainName, final boolean ipv6, final ResultCallback<InetAddress[]> callback) {
				checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

				if (HttpUtils.isInetAddress(domainName)) {
					callback.setResult(new InetAddress[]{inetAddress(domainName)});
					return;
				}

				DnsCacheQueryResult cacheQueryResult = cache.tryToResolve(domainName, ipv6, callback);

				if (cacheQueryResult == RESOLVED)
					return;

				if (cacheQueryResult == RESOLVED_NEEDS_REFRESHING) {
					AsyncDnsClient.this.eventloop.execute(new Runnable() {
						@Override
						public void run() {
							AsyncDnsClient.this.resolve(domainName, ipv6, IgnoreResultCallback.<InetAddress[]>create());
						}
					});
					return;
				}

				if (cacheQueryResult == NOT_RESOLVED) {
					AsyncDnsClient.this.eventloop.execute(new Runnable() {
						@Override
						public void run() {
							AsyncDnsClient.this.resolve(domainName, ipv6, ConcurrentResultCallback.create(callback, eventloop));
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
		checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

		if (HttpUtils.isInetAddress(domainName)) {
			callback.setResult(new InetAddress[]{HttpUtils.inetAddress(domainName)});
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
			protected void onResult(DnsQueryResult result) {
				if (callback != null && !resolvedFromCache) {
					callback.setResult(result.getIps());
				}
				cache.add(result);
				closeConnectionIfDone();
			}

			@Override
			protected void onException(Exception exception) {
				if (exception instanceof DnsException) {
					DnsException dnsException = (DnsException) exception;
					cache.add(dnsException);
				}
				if (callback != null && !resolvedFromCache) {
					callback.setException(exception);
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
			connection = DnsClientHandler.create(eventloop, datagramChannel);
			connection.register();
		} catch (IOException e) {
			if (logger.isErrorEnabled())
				logger.error("DnsClientConnection registration failed.", e);
		}
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	DnsCache getCache() {
		return cache;
	}

	// jmx
	@JmxAttribute
	public int getNumberOfCachedDomainNames() {
		return cache.getNumberOfCachedDomainNames();
	}

	@JmxAttribute
	public int getNumberOfCachedExceptions() {
		return cache.getNumberOfCachedExceptions();
	}

	@JmxAttribute
	public int getNumberOfQueriesInProgress() {
		if (connection == null) {
			return 0;
		} else {
			return connection.getNumberOfRequestsInProgress();
		}
	}

	@JmxAttribute
	public List<String> getDomainNamesBeingResolved() {
		if (connection == null) {
			return new ArrayList<>();
		} else {
			return asList(connection.getDomainNamesBeingResolved());
		}
	}

	@JmxAttribute
	public List<String> getAllCacheEntries() {
		return asList(cache.getAllCacheEntries());
	}

	@JmxAttribute
	public List<String> getSuccessfullyResolvedDomainNames() {
		return asList(cache.getSuccessfullyResolvedDomainNames());
	}

	@JmxAttribute
	public List<String> getDomainNamesOfFailedRequests() {
		return asList(cache.getDomainNamesOfFailedRequests());
	}

	@JmxAttribute(description = "max time to live for cache entry (resolved ip address for domain)")
	public long getMaxTtlMillis() {
		return cache.getMaxTtlMillis();
	}

	@JmxAttribute
	public void setMaxTtlMillis(long maxTtlMillis) {
		cache.setMaxTtlMillis(maxTtlMillis);
	}

	@JmxOperation
	public void emptyCache() {
		cache.clear();
	}
}
