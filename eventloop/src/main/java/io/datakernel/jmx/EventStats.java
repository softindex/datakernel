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

package io.datakernel.jmx;

import java.time.Duration;

import static java.lang.Math.exp;
import static java.lang.Math.log;

/**
 * Computes total amount of events and dynamic rate using exponential smoothing algorithm
 * <p>
 * Class is supposed to work in single thread
 */
public final class EventStats implements JmxRefreshableStats<EventStats>, JmxStatsWithSmoothingWindow, JmxStatsWithReset {
	private static final long TOO_LONG_TIME_PERIOD_BETWEEN_REFRESHES = Duration.ofHours(1).toMillis();
	private static final double LN_2 = log(2);

	private long lastTimestampMillis;
	private int lastCount;

	private long totalCount;
	private double smoothedCount;
	private double smoothedTimeSeconds;
	private double smoothedRate;

	private double smoothingWindow;
	private double smoothingWindowCoef;

	// fields for aggregation
	private int addedStats;

	// region builders
	private EventStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
	}

	private EventStats() {
		// create accumulator instance, smoothing window will be taken from actual stats
		this.smoothingWindow = -1;
		this.smoothingWindowCoef = -1;
	}

	public static EventStats createAccumulator() {
		return new EventStats();
	}

	/**
	 * Creates new EventStats with specified smoothing window
	 *
	 * @param smoothingWindow in seconds
	 */
	public static EventStats create(Duration smoothingWindow) {
		return new EventStats(smoothingWindow.toMillis() / 1000.0);
	}
	// endregion

	private static double calculateSmoothingWindowCoef(double smoothingWindow) {
		return -(LN_2 / smoothingWindow);
	}

	/**
	 * Resets rate to zero
	 */
	@Override
	public void resetStats() {
		lastCount = 0;
		totalCount = 0;
		lastTimestampMillis = 0;
		smoothedCount = 0;
		smoothedRate = 0;
		smoothedTimeSeconds = 0;
	}

	/**
	 * Records event and updates rate
	 */
	public void recordEvent() {
		lastCount++;
	}

	/**
	 * Records events and updates rate
	 *
	 * @param events number of events
	 */
	public void recordEvents(int events) {
		lastCount += events;
	}

	@Override
	public void refresh(long timestamp) {
		long timeElapsedMillis = timestamp - lastTimestampMillis;

		if (isTimePeriodValid(timeElapsedMillis)) {
			double timeElapsedSeconds = timeElapsedMillis * 0.001;
			double smoothingFactor = exp(timeElapsedSeconds * smoothingWindowCoef);
			smoothedCount = lastCount + smoothedCount * smoothingFactor;
			smoothedTimeSeconds = timeElapsedSeconds + smoothedTimeSeconds * smoothingFactor;
			smoothedRate = smoothedCount / smoothedTimeSeconds;
		} else {
			// skip stats of last time period
		}

		totalCount += lastCount;
		lastCount = 0;
		lastTimestampMillis = timestamp;
	}

	private static boolean isTimePeriodValid(long timePeriod) {
		return timePeriod < TOO_LONG_TIME_PERIOD_BETWEEN_REFRESHES && timePeriod > 0;
	}

	@Override
	public void add(EventStats anotherStats) {
		totalCount += anotherStats.totalCount;
		smoothedCount += anotherStats.smoothedCount;
		smoothedRate += anotherStats.smoothedRate;

		if (addedStats == 0) {
			smoothingWindow = anotherStats.smoothingWindow;
			smoothingWindowCoef = anotherStats.smoothingWindowCoef;
		} else {
			// all stats should have same smoothing window, -1 means smoothing windows differ in stats, which is error
			if (smoothingWindow != anotherStats.smoothingWindow) {
				smoothingWindow = -1;
				smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
			}
		}
		addedStats++;
	}

	/**
	 * Returns smoothed value of rate in events per second.
	 * <p>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed value of rate in events per second
	 */
	@JmxAttribute(optional = true)
	public double getSmoothedRate() {
		return smoothedRate;
	}

	/**
	 * Returns total amount of recorded events
	 *
	 * @return total amount of recorded events
	 */
	@JmxAttribute(optional = true)
	public long getTotalCount() {
		return totalCount;
	}

	@Override
	@JmxAttribute(optional = true)
	public Duration getSmoothingWindow() {
		return Duration.ofMillis((long) (smoothingWindow * 1000.0));
	}

	@Override
	@JmxAttribute(optional = true)
	public void setSmoothingWindow(Duration smoothingWindow) {
		this.smoothingWindow = smoothingWindow.toMillis() / 1000.0;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(this.smoothingWindow);
	}

	@JmxAttribute
	public String get() {
		return toString();
	}

	@Override
	public String toString() {
		if (getTotalCount() == 0) return null;
		return String.format("%d @ %.3f/s", getTotalCount(), getSmoothedRate());
	}
}
