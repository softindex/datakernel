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
import io.datakernel.async.SettableStage;
import io.datakernel.dns.DnsCache.DnsCacheResultStage;
import io.datakernel.eventloop.AsyncUdpSocketImpl;
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
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import static io.datakernel.dns.DnsCache.DnsCacheQueryResult.*;
import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;

/**
 * An implementation of DNS client which resolves IP addresses.
 * <p>
 * Instance of this class is capable to cache resolved addresses and able to
 * resolve IPv6 and IPv4 addresses.
 */
public final class AsyncDnsClient implements IAsyncDnsClient, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(AsyncDnsClient.class);

	private static final int DNS_SERVER_PORT = 53;
	private static final long ONE_MINUTE_MILLIS = 60 * 1000L;
	public static final DatagramSocketSettings DEFAULT_DATAGRAM_SOCKET_SETTINGS = DatagramSocketSettings.create();
	public static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress(inetAddress("8.8.8.8"), DNS_SERVER_PORT);
	public static final InetSocketAddress LOCAL_DNS = new InetSocketAddress(inetAddress("192.168.0.1"), DNS_SERVER_PORT);
	public static final long DEFAULT_TIMEOUT = 3_000L;
	public static final long DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS = 1_000L;

	private final Eventloop eventloop;

	private DnsClientConnection connection;
	private final DatagramSocketSettings datagramSocketSettings;

	private InetSocketAddress dnsServerAddress;

	private final DnsCache cache;

	private final long timeout;
	private final long timedOutExceptionTtl;

	// region builders
	public static AsyncDnsClient create(Eventloop eventloop) {
		DnsCache cache = DnsCache.create(eventloop, ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS, DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS);
		return new AsyncDnsClient(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, DEFAULT_TIMEOUT,
				DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS, GOOGLE_PUBLIC_DNS, cache);
	}

	public AsyncDnsClient withDatagramSocketSetting(DatagramSocketSettings setting) {
		return new AsyncDnsClient(eventloop, setting, timeout, timedOutExceptionTtl, dnsServerAddress, cache);
	}

	/**
	 * Creates a client which waits for result for specified timeout
	 *
	 * @param timeout time which this resolver will wait result
	 * @return a client, waiting for response for specified timeout
	 */
	public AsyncDnsClient withTimeout(long timeout) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, dnsServerAddress, cache);
	}

	public AsyncDnsClient withTimedOutExceptionTtl(long timedOutExceptionTtl) {
		DnsCache cache = DnsCache.create(eventloop, ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS, timedOutExceptionTtl);
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, dnsServerAddress, cache);
	}

	/**
	 * Creates a client with an address of server responsible for resolving
	 * domains names
	 *
	 * @param address address of DNS server which will resolve domain names
	 * @return a client with specified DNS server address
	 */
	public AsyncDnsClient withDnsServerAddress(InetSocketAddress address) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, address, cache);
	}

	/**
	 * Creates a client with an address of server responsible for resolving
	 * domains names
	 *
	 * @param address address of DNS server which will resolve domain names
	 * @return a client with specified DNS server address
	 */
	public AsyncDnsClient withDnsServerAddress(InetAddress address) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl,
				new InetSocketAddress(address, DNS_SERVER_PORT), cache);
	}

	public AsyncDnsClient withExpiration(long errorCacheExpirationMillis, long hardExpirationDeltaMillis) {
		return withExpiration(errorCacheExpirationMillis, hardExpirationDeltaMillis, DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS);
	}

	public AsyncDnsClient withExpiration(long errorCacheExpirationMillis, long hardExpirationDeltaMillis, long timedOutExceptionTtl) {
		DnsCache cache = DnsCache.create(eventloop, errorCacheExpirationMillis, hardExpirationDeltaMillis, timedOutExceptionTtl);
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, dnsServerAddress, cache);
	}
	// endregion

	private AsyncDnsClient(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout,
	                       long timedOutExceptionTtl, InetSocketAddress dnsServerAddress,
	                       long errorCacheExpirationMillis, long hardExpirationDeltaMillis) {
		this.eventloop = eventloop;
		this.datagramSocketSettings = datagramSocketSettings;
		this.timeout = timeout;
		this.timedOutExceptionTtl = timedOutExceptionTtl;
		this.dnsServerAddress = dnsServerAddress;
		this.cache = DnsCache.create(eventloop, errorCacheExpirationMillis, hardExpirationDeltaMillis, timedOutExceptionTtl);
	}

	private AsyncDnsClient(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout,
	                       long timedOutExceptionTtl, InetSocketAddress dnsServerAddress, DnsCache cache) {
		this.eventloop = eventloop;
		this.datagramSocketSettings = datagramSocketSettings;
		this.timeout = timeout;
		this.timedOutExceptionTtl = timedOutExceptionTtl;
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
			public CompletionStage<InetAddress[]> resolve4(String domainName) {
				return resolve(domainName, false);
			}

			@Override
			public CompletionStage<InetAddress[]> resolve6(String domainName) {
				return resolve(domainName, true);
			}

			private CompletionStage<InetAddress[]> resolve(final String domainName, final boolean ipv6) {
				checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

				if (HttpUtils.isInetAddress(domainName)) {
					return SettableStage.immediateStage(new InetAddress[]{inetAddress(domainName)});
				}

				DnsCacheResultStage<InetAddress[]> cacheQueryResult = cache.tryToResolve(domainName, ipv6);

				if (cacheQueryResult.getDnsCacheResult() == RESOLVED) return cacheQueryResult.getStage();

				if (cacheQueryResult.getDnsCacheResult() == RESOLVED_NEEDS_REFRESHING) {
					AsyncDnsClient.this.eventloop.execute(() -> AsyncDnsClient.this.resolve(domainName, ipv6));
					return cacheQueryResult.getStage();
				}

				final SettableStage<InetAddress[]> stage = SettableStage.create();
				if (cacheQueryResult.getDnsCacheResult() == NOT_RESOLVED) {
					AsyncDnsClient.this.eventloop.execute(() ->
							AsyncDnsClient.this.resolve(domainName, ipv6).whenComplete((inetAddresses, throwable) ->
							eventloop.execute(() -> AsyncCallbacks.forwardTo(stage, inetAddresses, throwable))));
				}

				return stage;
			}
		};
	}

	/**
	 * Resolves the IP for the IPv4 addresses and handles it with callback
	 *
	 * @param domainName domain name for searching IP
	 */
	@Override
	public CompletionStage<InetAddress[]> resolve4(final String domainName) {
		return resolve(domainName, false);
	}

	/**
	 * Resolves the IP for the IPv6 addresses and handles it with callback
	 *
	 * @param domainName domain name for searching IP
	 */
	@Override
	public CompletionStage<InetAddress[]> resolve6(String domainName) {
		return resolve(domainName, true);
	}

	private void closeConnectionIfDone() {
		if (connection != null && connection.allRequestsCompleted()) {
			connection.close();
			connection = null;
		}
	}

	private CompletionStage<InetAddress[]> resolve(final String domainName, final boolean ipv6) {
		checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

		if (HttpUtils.isInetAddress(domainName)) {
			return SettableStage.immediateStage(new InetAddress[]{HttpUtils.inetAddress(domainName)});
		}

		DnsCacheResultStage<InetAddress[]> cacheQueryResult = cache.tryToResolve(domainName, ipv6);

		if (cacheQueryResult.getDnsCacheResult() == RESOLVED) {
			cache.performCleanup();
			return cacheQueryResult.getStage();
		}

		final boolean resolvedFromCache = cacheQueryResult.getDnsCacheResult() == RESOLVED_NEEDS_REFRESHING;

		logger.trace("Resolving {} with DNS server.", domainName);

		final SettableStage<InetAddress[]> stage = SettableStage.create();
		final BiConsumer<DnsQueryResult, Throwable> queryConsumer = (dnsQueryResult, throwable) -> {
			if (throwable == null) {
				if (!resolvedFromCache) stage.setResult(dnsQueryResult.getIps());
				cache.add(dnsQueryResult);
				closeConnectionIfDone();
			} else {
				if (throwable instanceof DnsException) {
					DnsException dnsException = (DnsException) throwable;
					cache.add(dnsException);
				}
				if (!resolvedFromCache) stage.setError(throwable);
				closeConnectionIfDone();
			}
		};

		eventloop.post(() -> {
			if (connection == null) {
				try {
					registerConnection();
				} catch (IOException e) {
					if (logger.isErrorEnabled()) logger.error("DnsClientConnection registration failed.", e);
					queryConsumer.accept(null, e);
					return;
				}
			}

			if (ipv6) {
				connection.resolve6(domainName, dnsServerAddress, timeout).whenComplete(queryConsumer);
			} else {
				connection.resolve4(domainName, dnsServerAddress, timeout).whenComplete(queryConsumer);
			}
		});

		cache.performCleanup();
		return stage;
	}

	private void registerConnection() throws IOException {
		DatagramChannel datagramChannel = createDatagramChannel(datagramSocketSettings, null, dnsServerAddress);
		AsyncUdpSocketImpl udpSocket = AsyncUdpSocketImpl.create(eventloop, datagramChannel);
		connection = DnsClientConnection.create(eventloop, udpSocket);
		udpSocket.setEventHandler(connection);
		udpSocket.register();
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
	public long getMaxTtlSeconds() {
		return cache.getMaxTtlSeconds();
	}

	@JmxAttribute
	public void setMaxTtlSeconds(long maxTtlSeconds) {
		cache.setMaxTtlSeconds(maxTtlSeconds);
	}

	@JmxAttribute
	public long getTimedOutExceptionTtlSeconds() {
		return cache.getTimedOutExceptionTtlSeconds();
	}

	@JmxOperation
	public void emptyCache() {
		cache.clear();
	}
}
