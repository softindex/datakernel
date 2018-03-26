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

import io.datakernel.async.Callback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.dns.DnsCache.DnsCacheQueryResult;
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
import java.time.Duration;
import java.util.Arrays;

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
public final class AsyncDnsClient implements IAsyncDnsClient, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(AsyncDnsClient.class);

	private static final int DNS_SERVER_PORT = 53;
	private static final long ONE_MINUTE_MILLIS = 60 * 1000L;
	public static final DatagramSocketSettings DEFAULT_DATAGRAM_SOCKET_SETTINGS = DatagramSocketSettings.create();
	public static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress(inetAddress("8.8.8.8"), DNS_SERVER_PORT);
	public static final InetSocketAddress LOCAL_DNS = new InetSocketAddress(inetAddress("192.168.0.1"), DNS_SERVER_PORT);
	public static final long DEFAULT_TIMEOUT = 3_000L;
	public static final long DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS = 1_000L;

	private final Eventloop eventloop;

	private DnsClientSocketHandler socketHandler;
	private DatagramSocketSettings datagramSocketSettings = DEFAULT_DATAGRAM_SOCKET_SETTINGS;

	private InetSocketAddress dnsServerAddress = GOOGLE_PUBLIC_DNS;

	private DnsCache cache;

	private long timeout = DEFAULT_TIMEOUT;
	private long timedOutExceptionTtl = DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS;

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
	private AsyncDnsClient(Eventloop eventloop, DnsCache cache, Inspector inspector) {
		this.eventloop = eventloop;
		this.cache = cache;
		this.inspector = inspector;
	}

	public static AsyncDnsClient create(Eventloop eventloop) {
		Inspector inspector = new JmxInspector();
		DnsCache cache = DnsCache.create(eventloop, ONE_MINUTE_MILLIS, ONE_MINUTE_MILLIS, DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS, inspector);
		return new AsyncDnsClient(eventloop, cache, inspector);
	}

	public AsyncDnsClient withDatagramSocketSetting(DatagramSocketSettings setting) {
		this.datagramSocketSettings = setting;
		return this;
	}

	/**
	 * @see AsyncDnsClient#withTimeout(long)
	 */
	public AsyncDnsClient withTimeout(Duration timeout) {
		return withTimeout(timeout.toMillis());
	}

	/**
	 * Creates a client which waits for result for specified timeout
	 *
	 * @param timeout time which this resolver will wait result
	 * @return a client, waiting for response for specified timeout
	 */
	public AsyncDnsClient withTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	/**
	 * Creates a client with an address of server responsible for resolving
	 * domains names
	 *
	 * @param address address of DNS server which will resolve domain names
	 * @return a client with specified DNS server address
	 */
	public AsyncDnsClient withDnsServerAddress(InetSocketAddress address) {
		this.dnsServerAddress = address;
		return this;
	}

	/**
	 * Creates a client with an address of server responsible for resolving
	 * domains names
	 *
	 * @param address address of DNS server which will resolve domain names
	 * @return a client with specified DNS server address
	 */
	public AsyncDnsClient withDnsServerAddress(InetAddress address) {
		this.dnsServerAddress = new InetSocketAddress(address, DNS_SERVER_PORT);
		return this;
	}

	public AsyncDnsClient withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta) {
		return withExpiration(errorCacheExpiration.toMillis(), hardExpirationDelta.toMillis());
	}

	public AsyncDnsClient withExpiration(long errorCacheExpirationMillis, long hardExpirationDeltaMillis) {
		return withExpiration(errorCacheExpirationMillis, hardExpirationDeltaMillis, DEFAULT_TIMED_OUT_EXCEPTION_TTL_MILLIS);
	}

	public AsyncDnsClient withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta, Duration timedOutExceptionTtl) {
		return withExpiration(errorCacheExpiration.toMillis(), hardExpirationDelta.toMillis(), timedOutExceptionTtl.toMillis());
	}

	public AsyncDnsClient withExpiration(long errorCacheExpirationMillis, long hardExpirationDeltaMillis, long timedOutExceptionTtl) {
		this.cache = DnsCache.create(eventloop, errorCacheExpirationMillis, hardExpirationDeltaMillis, timedOutExceptionTtl, inspector);
		return this;
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
			public void resolve4(String domainName, Callback<InetAddress[]> callback) {
				resolve(domainName, false, callback);
			}

			@Override
			public void resolve6(String domainName, Callback<InetAddress[]> callback) {
				resolve(domainName, true, callback);
			}

			private void resolve(String domainName, boolean ipv6, Callback<InetAddress[]> callback) {
				checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

				if (HttpUtils.isInetAddress(domainName)) {
					callback.set(new InetAddress[]{inetAddress(domainName)});
					return;
				}

				DnsCacheQueryResult cacheQueryResult = cache.tryToResolve(domainName, ipv6, callback);

				if (cacheQueryResult == RESOLVED)
					return;

				if (cacheQueryResult == RESOLVED_NEEDS_REFRESHING) {
					AsyncDnsClient.this.eventloop.execute(() ->
							AsyncDnsClient.this.resolve(domainName, ipv6, Callback.ignore()));
					return;
				}

				assert cacheQueryResult == NOT_RESOLVED;
				AsyncDnsClient.this.eventloop.execute(() ->
						AsyncDnsClient.this.resolve(domainName, ipv6, new Callback<InetAddress[]>() {
							@Override
							public void set(InetAddress[] result) {
								eventloop.execute(() -> callback.set(result));
							}

							@Override
							public void setException(Throwable t) {
								eventloop.execute(() -> callback.setException(t));
							}
						}));

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
	public void resolve4(String domainName, Callback<InetAddress[]> callback) {
		resolve(domainName, false, callback);
	}

	/**
	 * Resolves the IP for the IPv6 addresses and handles it with callback
	 *
	 * @param domainName domain name for searching IP
	 * @param callback   result callback
	 */
	@Override
	public void resolve6(String domainName, Callback<InetAddress[]> callback) {
		resolve(domainName, true, callback);
	}

	private void resolve(String domainName, boolean ipv6, Callback<InetAddress[]> callback) {
		checkArgument(domainName != null && !domainName.isEmpty(), "Domain name cannot be null or empty");

		if (HttpUtils.isInetAddress(domainName)) {
			callback.set(new InetAddress[]{HttpUtils.inetAddress(domainName)});
			return;
		}

		DnsCacheQueryResult cacheQueryResult = cache.tryToResolve(domainName, ipv6, callback);

		if (cacheQueryResult == RESOLVED) {
			cache.performCleanup();
			return;
		}

		boolean resolvedFromCache = cacheQueryResult == RESOLVED_NEEDS_REFRESHING;

		logger.trace("Resolving {} with DNS server.", domainName);

		Callback<DnsQueryResult> queryCachingCallback = new Callback<DnsQueryResult>() {
			@Override
			public void set(DnsQueryResult result) {
				if (callback != null && !resolvedFromCache) {
					callback.set(result.getIps());
				}
				cache.add(result);
				closeSocketIfDone();
			}

			@Override
			public void setException(Throwable exception) {
				if (exception instanceof DnsException) {
					DnsException dnsException = (DnsException) exception;
					cache.add(dnsException);
				}
				if (callback != null && !resolvedFromCache) {
					callback.setException(exception);
				}
				closeSocketIfDone();
			}

			private void closeSocketIfDone() {
				if (socketHandler != null && socketHandler.allRequestsCompleted()) {
					socketHandler.close();
					socketHandler = null;
				}
			}
		};

		eventloop.post(() -> {
			if (socketHandler == null) {
				try {
					registerConnection();
				} catch (IOException e) {
					if (logger.isErrorEnabled()) logger.error("DnsClientConnection registration failed.", e);
					queryCachingCallback.setException(e);
					return;
				}
			}

			if (ipv6) {
				socketHandler.resolve6(domainName, dnsServerAddress, timeout, queryCachingCallback);
			} else {
				socketHandler.resolve4(domainName, dnsServerAddress, timeout, queryCachingCallback);
			}
		});

		cache.performCleanup();
	}

	private void registerConnection() throws IOException {
		DatagramChannel datagramChannel = createDatagramChannel(datagramSocketSettings, null, dnsServerAddress);
		AsyncUdpSocketImpl udpSocket = AsyncUdpSocketImpl.create(eventloop, datagramChannel)
				.withInspector(inspector != null ? inspector.socketInspector() : null);

		socketHandler = DnsClientSocketHandler.create(eventloop, udpSocket, inspector);
		udpSocket.setEventHandler(socketHandler);
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
		if (socketHandler == null) {
			return 0;
		} else {
			return socketHandler.getNumberOfRequestsInProgress();
		}
	}

	@JmxAttribute(description = "max time to live for cache entry (resolved ip address for domain)")
	public long getMaxTtlSeconds() {
		return cache.getMaxTtlSeconds();
	}

	@JmxAttribute
	public void setMaxTtlSeconds(long maxTtlSeconds) {
		cache.setMaxTtl(maxTtlSeconds);
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
		if (socketHandler == null) {
			return new String[0];
		} else {
			String[] domainNames = socketHandler.getDomainNamesBeingResolved();
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
