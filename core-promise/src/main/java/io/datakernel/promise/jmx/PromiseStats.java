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

package io.datakernel.promise.jmx;

import io.datakernel.async.callback.Callback;
import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.ExceptionStats;
import io.datakernel.eventloop.jmx.MBeanFormat;
import io.datakernel.eventloop.jmx.ValueStats;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.jmx.api.JmxReducers.JmxReducerSum;

/**
 * Allows to track stats of {@link Promise}s.
 */
public class PromiseStats {
	@Nullable
	private Eventloop eventloop;

	private int activePromises = 0;
	private long lastStartTimestamp = 0;
	private long lastCompleteTimestamp = 0;
	private final ValueStats duration;
	private final ExceptionStats exceptions = ExceptionStats.create();

	protected PromiseStats(@Nullable Eventloop eventloop, ValueStats duration) {
		this.eventloop = eventloop;
		this.duration = duration;
	}

	public static PromiseStats createMBean(Eventloop eventloop, Duration smoothingWindow) {
		return new PromiseStats(eventloop, ValueStats.create(smoothingWindow));
	}

	public static PromiseStats create(Duration smoothingWindow) {
		return new PromiseStats(null, ValueStats.create(smoothingWindow));
	}

	public PromiseStats withHistogram(int[] levels) {
		setHistogramLevels(levels);
		return this;
	}

	public void setHistogramLevels(int[] levels) {
		duration.setHistogramLevels(levels);
	}

	private long currentTimeMillis() {
		if (eventloop == null) {
			eventloop = getCurrentEventloop();
		}
		return eventloop.currentTimeMillis();
	}

	public <T> AsyncSupplier<T> wrapper(AsyncSupplier<T> callable) {
		return () -> monitor(callable.get());
	}

	public <T> Promise<T> monitor(Promise<T> promise) {
		return promise.whenComplete(recordStats());
	}

	public <T> Callback<T> recordStats() {
		activePromises++;
		long before = currentTimeMillis();
		lastStartTimestamp = before;
		return (value, e) -> {
			activePromises--;
			long now = currentTimeMillis();
			long durationMillis = now - before;
			lastCompleteTimestamp = now;
			duration.recordValue(durationMillis);

			if (e != null) {
				exceptions.recordException(e);
			}
		};
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getActivePromises() {
		return activePromises;
	}

	@JmxAttribute
	@Nullable
	public Instant getLastStartTime() {
		return lastStartTimestamp != 0L ? Instant.ofEpochMilli(lastStartTimestamp) : null;
	}

	@JmxAttribute
	@Nullable
	public Instant getLastCompleteTime() {
		return lastCompleteTimestamp != 0L ? Instant.ofEpochMilli(lastCompleteTimestamp) : null;
	}

	@JmxAttribute
	@Nullable
	public Duration getCurrentDuration() {
		return activePromises != 0 ? Duration.ofMillis(currentTimeMillis() - lastStartTimestamp) : null;
	}

	@JmxAttribute
	public ValueStats getDuration() {
		return duration;
	}

	@JmxAttribute
	public ExceptionStats getExceptions() {
		return exceptions;
	}

	@Override
	public String toString() {
		return "PromiseStats{" +
				"activePromises=" + activePromises +
				", lastStartTimestamp=" + MBeanFormat.formatTimestamp(lastStartTimestamp) +
				", lastCompleteTimestamp=" + MBeanFormat.formatTimestamp(lastCompleteTimestamp) +
				", duration=" + duration +
				", exceptions=" + exceptions +
				'}';
	}
}
