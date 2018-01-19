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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.dns.DnsCache.DnsCacheResultStage;
import io.datakernel.eventloop.AsyncUdpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.HttpUtils;
import io.datakernel.jmx.*;
import io.datakernel.net.DatagramSocketSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import static io.datakernel.dns.DnsCache.DnsCacheQueryResult.*;
import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.String.format;

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

	// jmx
	Inspector inspector;

	public interface Inspector {
		AsyncUdpSocketImpl.Inspector socketInspector();

		void onCacheHit(String domain, InetAddress[] ips);

		void onCacheHitError(String domain, DnsException exception);

		void onDnsQuery(String domain, ByteBuf query);

		void onDnsQueryResult(String domain, DnsQueryResult result);

		void onDnsQueryError(String domain, Throwable e);

		void onDomainExpired(String domain);
	}

	public static class JmxInspector implements Inspector {
		private static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;

		private final AsyncUdpSocketImpl.JmxInspector socketInspector =
				new AsyncUdpSocketImpl.JmxInspector(SMOOTHING_WINDOW);
		private final EventStats cacheHits = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats cacheHitErrors = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats queries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats failedQueries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats expirations = EventStats.create(SMOOTHING_WINDOW);

		@Override
		public AsyncUdpSocketImpl.Inspector socketInspector() {
			return socketInspector;
		}

		@Override
		public void onCacheHit(String domain, InetAddress[] ips) {
			cacheHits.recordEvent();
		}

		@Override
		public void onCacheHitError(String domain, DnsException exception) {
			cacheHitErrors.recordEvent();
		}

		@Override
		public void onDnsQuery(String domain, ByteBuf query) {
			queries.recordEvent();
		}

		@Override
		public void onDnsQueryResult(String domain, DnsQueryResult result) {
			if (!result.isSuccessful()) {
				failedQueries.recordEvent();
			}
		}

		@Override
		public void onDnsQueryError(String domain, Throwable e) {
			failedQueries.recordEvent();
		}

		@Override
		public void onDomainExpired(String domain) {
			expirations.recordEvent();
		}

		@JmxAttribute
		public AsyncUdpSocketImpl.JmxInspector getSocketInspector() {
			return socketInspector;
		}

		@JmxAttribute
		public EventStats getCacheHits() {
			return cacheHits;
		}

		@JmxAttribute
		public EventStats getCacheHitErrors() {
			return cacheHitErrors;
		}

		@JmxAttribute
		public EventStats getQueries() {
			return queries;
		}

		@JmxAttribute
		public EventStats getFailedQueries() {
			return failedQueries;
		}

		@JmxAttribute
		public EventStats getExpirations() {
			return expirations;
		}
	}

	// region builders
	private AsyncDnsClient(Eventloop eventloop, DatagramSocketSettings datagramSocketSettings, long timeout,
	                       long timedOutExceptionTtl, InetSocketAddress dnsServerAddress, DnsCache cache, Inspector inspector) {
		this.eventloop = eventloop;
		this.datagramSocketSettings = datagramSocketSettings;
		this.timeout = timeout;
		this.timedOutExceptionTtl = timedOutExceptionTtl;
		this.dnsServerAddress = dnsServerAddress;
		this.cache = cache;
		this.inspector = inspector;
	}

	public static AsyncDnsClient create(Eventloop eventloop) {
		Inspector inspector = new JmxInspector();
		DnsCache cache = DnsCache.create(eventloop, ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS, DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS, inspector);

		return new AsyncDnsClient(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, DEFAULT_TIMEOUT,
				DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS, GOOGLE_PUBLIC_DNS, cache, inspector);
	}

	public AsyncDnsClient withDatagramSocketSetting(DatagramSocketSettings setting) {
		return new AsyncDnsClient(eventloop, setting, timeout, timedOutExceptionTtl, dnsServerAddress, cache, inspector);
	}

	/**
	 * Creates a client which waits for result for specified timeout
	 *
	 * @param timeout time which this resolver will wait result
	 * @return a client, waiting for response for specified timeout
	 */
	public AsyncDnsClient withTimeout(long timeout) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, dnsServerAddress, cache, inspector);
	}

	public AsyncDnsClient withTimedOutExceptionTtl(long timedOutExceptionTtl) {
		DnsCache cache = DnsCache.create(eventloop, ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS, timedOutExceptionTtl, inspector);
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, dnsServerAddress, cache, inspector);
	}

	/**
	 * Creates a client with an address of server responsible for resolving
	 * domains names
	 *
	 * @param address address of DNS server which will resolve domain names
	 * @return a client with specified DNS server address
	 */
	public AsyncDnsClient withDnsServerAddress(InetSocketAddress address) {
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, address, cache, inspector);
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
				new InetSocketAddress(address, DNS_SERVER_PORT), cache, inspector);
	}

	public AsyncDnsClient withExpiration(long errorCacheExpirationMillis, long hardExpirationDeltaMillis) {
		return withExpiration(errorCacheExpirationMillis, hardExpirationDeltaMillis, DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS);
	}

	public AsyncDnsClient withExpiration(long errorCacheExpirationMillis, long hardExpirationDeltaMillis, long timedOutExceptionTtl) {
		DnsCache cache = DnsCache.create(eventloop, errorCacheExpirationMillis, hardExpirationDeltaMillis, timedOutExceptionTtl, inspector);
		return new AsyncDnsClient(eventloop, datagramSocketSettings, timeout, timedOutExceptionTtl, dnsServerAddress, cache, inspector);
	}

	public AsyncDnsClient withInspector(AsyncDnsClient.Inspector inspector) {
		this.inspector = inspector;
		this.cache.setInspector(inspector);
		return this;
	}
	// endregion

	/**
	 * Returns the DNS adapted client which will run in other eventloop using the same DNS cache
	 *
	 * @param eventloop eventloop in which DnsClient will be ran
	 * @return DNS client which will run in other eventloop
	 */
	public IAsyncDnsClient adaptToAnotherEventloop(Eventloop eventloop) {
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

			private CompletionStage<InetAddress[]> resolve(String domainName, boolean ipv6) {
				checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

				if (HttpUtils.isInetAddress(domainName)) {
					return Stages.of(new InetAddress[]{inetAddress(domainName)});
				}

				DnsCacheResultStage<InetAddress[]> cacheQueryResult = cache.tryToResolve(domainName, ipv6);

				if (cacheQueryResult.getDnsCacheResult() == RESOLVED) return cacheQueryResult.getStage();

				if (cacheQueryResult.getDnsCacheResult() == RESOLVED_NEEDS_REFRESHING) {
					AsyncDnsClient.this.eventloop.execute(() -> AsyncDnsClient.this.resolve(domainName, ipv6));
					return cacheQueryResult.getStage();
				}

				SettableStage<InetAddress[]> stage = SettableStage.create();
				if (cacheQueryResult.getDnsCacheResult() == NOT_RESOLVED) {
					AsyncDnsClient.this.eventloop.execute(() ->
							AsyncDnsClient.this.resolve(domainName, ipv6).whenComplete((inetAddresses, throwable) ->
									eventloop.execute(() -> stage.set(inetAddresses, throwable))));
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
	public CompletionStage<InetAddress[]> resolve4(String domainName) {
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

	private CompletionStage<InetAddress[]> resolve(String domainName, boolean ipv6) {
		checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

		if (HttpUtils.isInetAddress(domainName)) {
			return Stages.of(new InetAddress[]{HttpUtils.inetAddress(domainName)});
		}

		DnsCacheResultStage<InetAddress[]> cacheQueryResult = cache.tryToResolve(domainName, ipv6);

		if (cacheQueryResult.getDnsCacheResult() == RESOLVED) {
			cache.performCleanup();
			return cacheQueryResult.getStage();
		}

		boolean resolvedFromCache = cacheQueryResult.getDnsCacheResult() == RESOLVED_NEEDS_REFRESHING;

		logger.trace("Resolving {} with DNS server.", domainName);

		SettableStage<InetAddress[]> stage = SettableStage.create();
		BiConsumer<DnsQueryResult, Throwable> queryConsumer = (dnsQueryResult, throwable) -> {
			if (throwable == null) {
				if (!resolvedFromCache) stage.set(dnsQueryResult.getIps());
				cache.add(dnsQueryResult);
				closeConnectionIfDone();
			} else {
				if (throwable instanceof DnsException) {
					DnsException dnsException = (DnsException) throwable;
					cache.add(dnsException);
				}
				if (!resolvedFromCache) stage.setException(throwable);
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
		AsyncUdpSocketImpl udpSocket = AsyncUdpSocketImpl.create(eventloop, datagramChannel)
				.withInspector(inspector != null ? inspector.socketInspector() : null);

		connection = DnsClientConnection.create(eventloop, udpSocket, inspector);
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

	// region jmx
	@JmxAttribute
	public int getCachedDomainNames() {
		return cache.getNumberOfCachedDomainNames();
	}

	@JmxAttribute
	public int getCachedExceptions() {
		return cache.getNumberOfCachedExceptions();
	}

	@JmxAttribute
	public int getQueriesInProgress() {
		if (connection == null) {
			return 0;
		} else {
			return connection.getNumberOfRequestsInProgress();
		}
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

	@JmxAttribute(name = "")
	public AsyncDnsClient.JmxInspector getStats() {
		return inspector instanceof AsyncDnsClient.JmxInspector ?
				(AsyncDnsClient.JmxInspector) inspector :
				null;
	}

	@JmxOperation
	public String[] getDomainNamesBeingResolved(@JmxParameter("offset") int offset,
	                                            @JmxParameter("maxSize") int maxSize) {
		if (connection == null) {
			return new String[0];
		} else {
			String[] domainNames = connection.getDomainNamesBeingResolved();
			if (offset > domainNames.length) {
				throw new IllegalArgumentException(format(
						"There are only %d domain names being resolved. But requested offset was %d ",
						domainNames.length, offset)
				);
			}
			return Arrays.copyOfRange(domainNames, offset, Math.min(offset + maxSize, domainNames.length));
		}
	}

	@JmxOperation
	public String[] getAllCacheEntries(@JmxParameter("offset") int offset,
	                                   @JmxParameter("maxSize") int maxSize) {
		String[] cacheEntriesWithHeaderLine = cache.getAllCacheEntriesWithHeaderLine();

		if (cacheEntriesWithHeaderLine.length == 0) {
			return new String[0];
		}

		String header = cacheEntriesWithHeaderLine[0];
		String[] cacheEntries =
				Arrays.copyOfRange(cacheEntriesWithHeaderLine, 1, cacheEntriesWithHeaderLine.length);

		if (offset > cacheEntries.length) {
			throw new IllegalArgumentException(format(
					"There are only %d cache entries. But requested offset was %d ",
					cacheEntries.length, offset)
			);
		}

		int size = Math.min(maxSize, cacheEntries.length - offset);
		String[] resultArray = new String[size + 1];
		resultArray[0] = header;
		System.arraycopy(cacheEntries, offset, resultArray, 1, size);

		return resultArray;
	}

	@JmxOperation
	public String[] getSuccessfullyResolvedDomainNames(@JmxParameter("offset") int offset,
	                                                   @JmxParameter("maxSize") int maxSize) {
		String[] domainNames = cache.getSuccessfullyResolvedDomainNames();
		if (offset > domainNames.length) {
			throw new IllegalArgumentException(format(
					"There are only %d of successfully resolved domain names. But requested offset was %d ",
					domainNames.length, offset)
			);
		}
		return Arrays.copyOfRange(domainNames, offset, Math.min(offset + maxSize, domainNames.length));
	}

	@JmxOperation
	public String[] getDomainNamesOfFailedRequests(@JmxParameter("offset") int offset,
	                                               @JmxParameter("maxSize") int maxSize) {
		String[] domainNames = cache.getDomainNamesOfFailedRequests();
		if (offset > domainNames.length) {
			throw new IllegalArgumentException(format(
					"There are only %d domain names of failed requests. But requested offset was %d ",
					domainNames.length, offset)
			);
		}
		return Arrays.copyOfRange(domainNames, offset, Math.min(offset + maxSize, domainNames.length));
	}

	@JmxOperation
	public void emptyCache() {
		cache.clear();
	}
	// endregion
}
