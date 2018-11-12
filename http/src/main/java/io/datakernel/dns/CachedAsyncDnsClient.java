/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.dns.DnsCache.DnsQueryCacheResult;
import io.datakernel.eventloop.AsyncUdpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.datakernel.dns.DnsCache.DEFAULT_TIMED_OUT_EXCEPTION_TTL;

public class CachedAsyncDnsClient implements AsyncDnsClient, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(CachedAsyncDnsClient.class);

	private final Eventloop eventloop;
	private final AsyncDnsClient client;

	private DnsCache cache;
	private final Map<DnsQuery, MaterializedPromise<DnsResponse>> pending = new HashMap<>();
	private final Set<DnsQuery> refreshingNow = Collections.newSetFromMap(new ConcurrentHashMap<>());

	private CachedAsyncDnsClient(Eventloop eventloop, AsyncDnsClient client, DnsCache cache) {
		this.eventloop = eventloop;
		this.client = client;
		this.cache = cache;
	}

	public static CachedAsyncDnsClient create(Eventloop eventloop, AsyncDnsClient client, DnsCache cache) {
		return new CachedAsyncDnsClient(eventloop, client, cache);
	}

	public static CachedAsyncDnsClient create(Eventloop eventloop, AsyncDnsClient client) {
		return new CachedAsyncDnsClient(eventloop, client, DnsCache.create(eventloop));
	}

	public CachedAsyncDnsClient withCache(DnsCache cache) {
		this.cache = cache;
		return this;
	}

	public CachedAsyncDnsClient withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta) {
		return withExpiration(errorCacheExpiration, hardExpirationDelta, DEFAULT_TIMED_OUT_EXCEPTION_TTL);
	}

	public CachedAsyncDnsClient withExpiration(Duration errorCacheExpiration, Duration hardExpirationDelta, Duration timedOutExceptionTtl) {
		cache
				.withErrorCacheExpirationSeconds(errorCacheExpiration)
				.withHardExpirationDelta(hardExpirationDelta)
				.withTimedOutExceptionTtl(timedOutExceptionTtl);
		return this;
	}

	public DnsCache getCache() {
		return cache;
	}

	public AsyncDnsClient adaptToOtherEventloop(Eventloop other) {
		if (other == eventloop) {
			return this;
		}
		return new AsyncDnsClient() {
			@Override
			public MaterializedPromise<DnsResponse> resolve(DnsQuery query) {
				DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
				if (fromQuery != null) {
					logger.trace("{} already contained an IP address within itself", query);
					return Promise.of(fromQuery);
				}

				DnsQueryCacheResult cacheResult = cache.tryToResolve(query);
				if (cacheResult != null) {
					if (cacheResult.doesNeedRefreshing() && !refreshingNow.add(query)) {
						eventloop.execute(() -> refresh(query));
					}
					return cacheResult.getResponseAsPromise();
				}

				SettablePromise<DnsResponse> promise = new SettablePromise<>();
				other.startExternalTask(); // keep other eventloop alive while we wait for an answer in main one
				eventloop.execute(() ->
						CachedAsyncDnsClient.this.resolve(query)
								.whenComplete((result, e) -> {
									other.execute(() -> promise.set(result, e));
									other.completeExternalTask();
								}));
				return promise;
			}

			@Override
			public void close() {
				eventloop.execute(CachedAsyncDnsClient.this::close);
			}
		};
	}

	private void addToCache(DnsQuery query, DnsResponse response, @Nullable Throwable e) {
		if (e == null) {
			cache.add(query, response);
		} else if (e instanceof DnsQueryException) {
			cache.add(query, ((DnsQueryException) e).getResult());
		}
	}

	private void refresh(DnsQuery query) {
		if (!refreshingNow.add(query)) {
			logger.trace("{} needs refreshing, but it does so right now", query);
			return;
		}
		logger.trace("Refreshing {}", query);
		client.resolve(query)
				.whenComplete((response, e) -> {
					addToCache(query, response, e);
					refreshingNow.remove(query);
				});
	}

	@Override
	public MaterializedPromise<DnsResponse> resolve(DnsQuery query) {
		assert eventloop.inEventloopThread() : "Concurrent resolves are not allowed, to reuse the cache use adaptToOtherEventloop";

		DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
		if (fromQuery != null) {
			logger.trace("{} already contained an IP address within itself", query);
			return Promise.of(fromQuery);
		}

		logger.trace("Resolving {}", query);
		DnsQueryCacheResult cacheResult = cache.tryToResolve(query);
		if (cacheResult != null) {
			if (cacheResult.doesNeedRefreshing()) {
				refresh(query);
			}
			return cacheResult.getResponseAsPromise();
		}
		MaterializedPromise<DnsResponse> promise = pending.compute(query, (k, v) -> {
			if (v != null) {
				logger.trace("{} is already pending", k);
				return v;
			}
			MaterializedPromise<DnsResponse> resolve = client.resolve(k);
			resolve.whenComplete((response, e) -> {
				addToCache(k, response, e);
				pending.remove(k);
			});
			return resolve;
		});
		cache.performCleanup();
		return promise;
	}

	@Override
	public void close() {
		client.close();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	// region JMX
	public static class JmxInspector implements RemoteAsyncDnsClient.Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final AsyncUdpSocketImpl.JmxInspector socketInspector =
				new AsyncUdpSocketImpl.JmxInspector(SMOOTHING_WINDOW);
		private final EventStats queries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats failedQueries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats expirations = EventStats.create(SMOOTHING_WINDOW);

		@Override
		@Nullable
		public AsyncUdpSocketImpl.Inspector socketInspector() {
			return socketInspector;
		}

		@Override
		public void onDnsQuery(DnsQuery query, ByteBuf payload) {
			queries.recordEvent();
		}

		@Override
		public void onDnsQueryResult(DnsQuery query, DnsResponse result) {
			if (!result.isSuccessful()) {
				failedQueries.recordEvent();
			}
		}

		@Override
		public void onDnsQueryError(DnsQuery query, Throwable e) {
			failedQueries.recordEvent();
		}

		@JmxAttribute
		public AsyncUdpSocketImpl.JmxInspector getSocketInspector() {
			return socketInspector;
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
	// endregion
}
