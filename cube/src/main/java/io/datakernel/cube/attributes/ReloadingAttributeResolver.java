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

package io.datakernel.cube.attributes;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.ValueStats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_1_HOUR;

public abstract class ReloadingAttributeResolver<K, A> extends AbstractAttributeResolver<K, A> implements EventloopService {
	protected final Eventloop eventloop;

	private long timestamp;
	private long reloadPeriod;
	private long retryPeriod = 1000L;
	private ScheduledRunnable scheduledRunnable;
	private final Map<K, A> cache = new HashMap<>();
	private int reloads;
	private int reloadErrors;
	private int resolveErrors;
	private K lastResolveErrorKey;
	private final ValueStats reloadTime = ValueStats.create(SMOOTHING_WINDOW_1_HOUR);

	protected ReloadingAttributeResolver(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@Override
	protected final A resolveAttributes(K key) {
		A result = cache.get(key);
		if (result == null) {
			resolveErrors++;
			lastResolveErrorKey = key;
		}
		return result;
	}

	protected abstract CompletionStage<Map<K, A>> reload(@Nullable long lastTimestamp);

	private void doReload() {
		reloads++;
		scheduledRunnable.cancel();
		long reloadTimestamp = getEventloop().currentTimeMillis();
		CompletionStage<Map<K, A>> reload = reload(timestamp).whenComplete((result, throwable) -> {
			if (throwable == null) {
				reloadTime.recordValue((int) (getEventloop().currentTimeMillis() - reloadTimestamp));
				cache.putAll(result);
				timestamp = reloadTimestamp;
				scheduleReload(reloadPeriod);
			} else {
				reloadErrors++;
				scheduleReload(retryPeriod);
			}
		});
	}

	private void scheduleReload(long period) {
		Eventloop eventloop = getEventloop();
		scheduledRunnable = eventloop.schedule(eventloop.currentTimeMillis() + period, new Runnable() {
			@Override
			public void run() {
				doReload();
			}
		});
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public CompletionStage<Void> start() {
		if (reloadPeriod == 0) return Stages.of(null);
		long reloadTimestamp = getEventloop().currentTimeMillis();
		return reload(timestamp).thenAccept(result -> {
			reloadTime.recordValue((int) (getEventloop().currentTimeMillis() - reloadTimestamp));
			cache.putAll(result);
			timestamp = reloadTimestamp;
			scheduleReload(reloadPeriod);
		});
	}

	@Override
	public CompletionStage<Void> stop() {
		if (scheduledRunnable != null) scheduledRunnable.cancel();
		return Stages.of(null);
	}

	@JmxOperation
	public void reload() {
		doReload();
	}

	@JmxAttribute
	public long getReloadPeriod() {
		return reloadPeriod;
	}

	@JmxAttribute
	public void setReloadPeriod(long reloadPeriod) {
		this.reloadPeriod = reloadPeriod;
	}

	@JmxAttribute
	public long getRetryPeriod() {
		return retryPeriod;
	}

	@JmxAttribute
	public void setRetryPeriod(long retryPeriod) {
		this.retryPeriod = retryPeriod;
	}

	@JmxAttribute
	public int getReloads() {
		return reloads;
	}

	@JmxAttribute
	public int getReloadErrors() {
		return reloadErrors;
	}

	@JmxAttribute
	public int getResolveErrors() {
		return resolveErrors;
	}

	@JmxAttribute
	public K getLastResolveErrorKey() {
		return lastResolveErrorKey;
	}

	@JmxAttribute
	public ValueStats getReloadTime() {
		return reloadTime;
	}
}
