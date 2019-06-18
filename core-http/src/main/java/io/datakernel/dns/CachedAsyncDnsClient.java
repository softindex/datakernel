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

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.dns.DnsCache.DnsQueryCacheResult;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static io.datakernel.dns.DnsCache.DEFAULT_TIMED_OUT_EXCEPTION_TTL;
import static java.util.logging.Level.*;

public class CachedAsyncDnsClient implements AsyncDnsClient, EventloopJmxMBeanEx {
	private final Logger logger = Logger.getLogger(CachedAsyncDnsClient.class.getName());

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
					logger.log(FINE, () -> query + " already contained an IP address within itself");
					return Promise.of(fromQuery);
				}

				DnsQueryCacheResult cacheResult = cache.tryToResolve(query);
				if (cacheResult != null) {
					if (cacheResult.doesNeedRefreshing() && !refreshingNow.add(query)) {
						eventloop.execute(() -> refresh(query));
					}
					return cacheResult.getResponseAsPromise();
				}

				other.startExternalTask(); // keep other eventloop alive while we wait for an answer in main one
				return Promise.ofCallback(cb ->
								eventloop.execute(() ->
										CachedAsyncDnsClient.this.resolve(query)
												.whenComplete((result, e) -> {
													other.execute(() -> cb.accept(result, e));
													other.completeExternalTask();
												})));
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
			logger.log(FINEST, () -> query + " needs refreshing, but it does so right now");
			return;
		}
		logger.log(FINER, () -> "Refreshing " + query);
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
			logger.log(FINE, () -> query + " already contained an IP address within itself");
			return Promise.of(fromQuery);
		}

		logger.log(FINER, () -> "Resolving " + query);
		DnsQueryCacheResult cacheResult = cache.tryToResolve(query);
		if (cacheResult != null) {
			if (cacheResult.doesNeedRefreshing()) {
				refresh(query);
			}
			return cacheResult.getResponseAsPromise();
		}
		MaterializedPromise<DnsResponse> promise = pending.compute(query, (k, v) -> {
			if (v != null) {
				logger.log(FINE, () -> k + " is already pending");
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

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

}
