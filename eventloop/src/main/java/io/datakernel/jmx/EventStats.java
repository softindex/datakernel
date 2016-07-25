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

import static java.lang.Math.exp;
import static java.lang.Math.log;

/**
 * Computes total amount of events and dynamic rate using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class EventStats implements JmxRefreshableStats<EventStats> {
	private static final long TOO_LONG_TIME_PERIOD_BETWEEN_REFRESHES = 60 * 1000; // 1 minute
	private static final double DEFAULT_SMOOTHING_WINDOW = 10.0;
	private static final double LN_2 = log(2);

	private long lastTimestampMillis;
	private int lastCount;

	private long totalCount;
	private double smoothedCount;
	private double smoothedTimeSeconds;
	private double smoothedRate;

	private double smoothingWindow;

	// fields for aggregation
	private int addedStats;

	public EventStats() {
		this.smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
	}

	public EventStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
	}

	/**
	 * Resets rate to zero
	 */
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
		recordEvents(1);
	}

	/**
	 * Records events and updates rate
	 *
	 * @param events number of events
	 */
	public void recordEvents(int events) {
		lastCount += events;
		totalCount += events;
	}

	@Override
	public void refresh(long timestamp) {
		long timeElapsedMillis = timestamp - lastTimestampMillis;

		if (isTimePeriodValid(timeElapsedMillis)) {

			double timeElapsedSeconds = timeElapsedMillis / 1000.0;
			double smoothingFactor = exp(-(timeElapsedSeconds / smoothingWindow) * LN_2);
			smoothedCount = lastCount + smoothedCount * smoothingFactor;
			smoothedTimeSeconds =
					timeElapsedSeconds + smoothedTimeSeconds * smoothingFactor;

			smoothedRate = smoothedCount / smoothedTimeSeconds;
		} else {
			// skip stats of last time period
		}

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
		} else {
			// all stats should have same smoothing window, -1 means smoothing windows differ in stats, which is error
			if (smoothingWindow != anotherStats.smoothingWindow) {
				smoothingWindow = -1;
			}
		}
		addedStats++;
	}

	/**
	 * Returns smoothed value of rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed value of rate in events per second
	 */
	@JmxAttribute
	public double getSmoothedRate() {
		return smoothedRate;
	}

	/**
	 * Returns total amount of recorded events
	 *
	 * @return total amount of recorded events
	 */
	@JmxAttribute
	public long getTotalCount() {
		return totalCount;
	}

	@JmxAttribute(optional = true)
	public double getSmoothingWindow() {
		return smoothingWindow;
	}

	@JmxAttribute
	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
	}

	@Override
	public String toString() {
		return String.format("total: %d   smoothedRate: %.3f",
				getTotalCount(), getSmoothedRate());
	}
}
