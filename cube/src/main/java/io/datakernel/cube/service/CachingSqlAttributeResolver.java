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

package io.datakernel.cube.service;

import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import io.datakernel.aggregation_db.PrimaryKey;
import io.datakernel.async.CompletionCallback;
import io.datakernel.cube.api.AttributeResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Maps.newHashMap;

public abstract class CachingSqlAttributeResolver implements AttributeResolver, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(CachingSqlAttributeResolver.class);

	protected final Eventloop eventloop;
	protected final ExecutorService executor;
	protected final Configuration jooqConfiguration;
	protected final Map<PrimaryKey, Object[]> cache = newHashMap();
	protected final ObjectIntMap<String> attributeIndices = new ObjectIntOpenHashMap<>();

	private long previousUpdateTimestamp = -1;
	private final long updateDelayMillis;

	private static final long DEFAULT_UPDATE_DELAY_MILLIS = 60 * 1000;

	private ScheduledRunnable scheduledUpdateTask;
	private final long refreshPeriodMillis;
	private final Runnable updateTask = createUpdateTask();

	protected CachingSqlAttributeResolver(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration,
	                                      long refreshPeriodMillis) {
		this(eventloop, executor, jooqConfiguration, refreshPeriodMillis, DEFAULT_UPDATE_DELAY_MILLIS);
	}

	protected CachingSqlAttributeResolver(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration,
	                                      long refreshPeriodMillis, long updateDelayMillis) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
		this.refreshPeriodMillis = refreshPeriodMillis;
		this.updateDelayMillis = updateDelayMillis;
	}

	protected Timestamp getPreviousUpdateTimestamp() {
		return new Timestamp(previousUpdateTimestamp - updateDelayMillis);
	}

	private Runnable createUpdateTask() {
		return new Runnable() {
			@Override
			public void run() {
				updateCache(new CompletionCallback() {
					@Override
					public void onComplete() {
						previousUpdateTimestamp = eventloop.currentTimeMillis();
						scheduleUpdate();
					}

					@Override
					public void onException(Exception e) {
						logger.warn("Updating cache failed", e);
						scheduleUpdate();
					}
				});
			}
		};
	}

	private void scheduleUpdate() {
		if (scheduledUpdateTask != null && scheduledUpdateTask.isCancelled())
			return;

		scheduledUpdateTask = eventloop.scheduleBackground(eventloop.currentTimeMillis() + refreshPeriodMillis, updateTask);
	}

	public abstract void fillCache(CompletionCallback callback);

	public abstract void updateCache(CompletionCallback callback);

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		fillCache(new CompletionCallback() {
			@Override
			public void onComplete() {
				previousUpdateTimestamp = eventloop.currentTimeMillis();
				scheduleUpdate();
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void stop(CompletionCallback callback) {
		if (scheduledUpdateTask != null)
			scheduledUpdateTask.cancel();

		callback.onComplete();
	}

	@Override
	public Map<PrimaryKey, Object[]> resolve(Set<PrimaryKey> keys, List<String> attributes) {
		return resolveFromCache(keys, attributes);
	}

	protected Map<PrimaryKey, Object[]> resolveFromCache(Set<PrimaryKey> keys, List<String> attributes) {
		Map<PrimaryKey, Object[]> resolvedAttributes = newHashMap();

		for (PrimaryKey key : keys) {
			Object[] cachedValues = cache.get(key);
			Object[] requestedValues = new Object[attributes.size()];
			for (int i = 0; i < attributes.size(); ++i) {
				requestedValues[i] = cachedValues == null ? null : cachedValues[attributeIndices.get(attributes.get(i))];
			}
			resolvedAttributes.put(key, requestedValues);
		}

		return resolvedAttributes;
	}

	protected void cacheIntegerStringRecords(Result<Record2<Integer, String>> records) {
		for (Record2<Integer, String> record : records) {
			cacheRecord(record);
		}
	}

	protected void cacheStringStringRecords(Result<Record2<String, String>> records) {
		for (Record2<String, String> record : records) {
			cacheRecord(record);
		}
	}

	protected void addToCache(PrimaryKey key, Object... values) {
		cache.put(key, values);
	}

	private void cacheRecord(Record2 record) {
		addToCache(PrimaryKey.ofArray(record.value1()), record.value2());
	}

	protected DSLContext jooq() {
		return DSL.using(jooqConfiguration);
	}
}
