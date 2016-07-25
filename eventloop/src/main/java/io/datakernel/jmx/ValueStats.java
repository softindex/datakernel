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

import static java.lang.Math.*;

/**
 * Counts added values and computes dynamic average using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class ValueStats implements JmxRefreshableStats<ValueStats> {
	private static final long TOO_LONG_TIME_PERIOD_BETWEEN_REFRESHES = 60 * 1000; // 1 minute
	private static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private long lastTimestampMillis;

	private int lastValue;
	private int lastSum;
	private int lastSqr;
	private int lastCount;
	private int lastMin;
	private int lastMax;

	private int totalMin;
	private int totalMax;
	private long totalSum;
	private long totalCount;

	private double smoothedSum;
	private double smoothedSqr;
	private double smoothedCount;

	private double smoothingWindow;

	// fields for aggregation
	private int addedStats;

	/**
	 * Resets stats and sets new parameters
	 */
	public void resetStats() {
		smoothedSum = 0.0;
		smoothedSqr = 0.0;
		smoothedCount = 0.0;
		totalMax = 0;
		totalMin = 0;
		totalSum = 0;
		totalCount = 0;
		lastMax = Integer.MIN_VALUE;
		lastMin = Integer.MAX_VALUE;
		lastSum = 0;
		lastSqr = 0;
		lastCount = 0;
		lastValue = 0;
		lastTimestampMillis = 0L;
	}

	public ValueStats() {
		this.smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
	}

	public ValueStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
	}

	/**
	 * Adds value
	 */
	public void recordValue(int value) {
		lastValue = value;

		if (value < lastMin) {
			lastMin = value;
		}

		if (value > lastMax) {
			lastMax = value;
		}

		lastSum += value;
		lastSqr += value * value;
		lastCount++;
	}

	@Override
	public void refresh(long timestamp) {
		if (lastCount == 0)
			return;

		if (lastTimestampMillis == 0L) {
			smoothedSum = lastSum;
			smoothedSqr = lastSqr;
			smoothedCount = lastCount;
		} else {
			long timeElapsedMillis = timestamp - lastTimestampMillis;

			if (isTimePeriodValid(timeElapsedMillis)) {
				double windowE = smoothingWindow * 1000.0 / log(2);
				double weight = exp(-(timeElapsedMillis) / windowE);

				smoothedSum = lastSum + smoothedSum * weight;
				smoothedSqr = lastSqr + smoothedSqr * weight;
				smoothedCount = lastCount + smoothedCount * weight;

				totalSum += lastSum;
				totalCount += lastCount;

				if (lastMin < totalMin) {
					totalMin = lastMin;
				}

				if (lastMax > totalMax) {
					totalMax = lastMax;
				}
			} else {
				// skip stats of last time period
			}
		}

		lastTimestampMillis = timestamp;

		lastSum = 0;
		lastSqr = 0;
		lastCount = 0;
		lastMin = Integer.MAX_VALUE;
		lastMax = Integer.MIN_VALUE;
	}

	private static boolean isTimePeriodValid(long timePeriod) {
		return timePeriod < TOO_LONG_TIME_PERIOD_BETWEEN_REFRESHES && timePeriod > 0;
	}

	@Override
	public void add(ValueStats anotherStats) {
		if (anotherStats.lastTimestampMillis == 0L)
			return;

		smoothedSum += anotherStats.smoothedSum;
		smoothedSqr += anotherStats.smoothedSqr;
		smoothedCount += anotherStats.smoothedCount;

		totalSum += anotherStats.totalSum;
		totalCount += anotherStats.totalCount;
		if (anotherStats.totalMin < totalMin) {
			totalMin = anotherStats.totalMin;
		}
		if (anotherStats.totalMax > totalMax) {
			totalMax = anotherStats.totalMax;
		}

		if (anotherStats.lastTimestampMillis > lastTimestampMillis) {
			lastTimestampMillis = anotherStats.lastTimestampMillis;
			lastValue = anotherStats.lastValue;
		}

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
	 * Returns last added value
	 *
	 * @return last added value
	 */
	@JmxAttribute
	public int getLastValue() {
		return lastValue;
	}

	/**
	 * Returns smoothed average of added values
	 *
	 * @return smoothed average of added values
	 */
	@JmxAttribute
	public double getSmoothedAverage() {
		if (totalCount == 0) {
			return 0.0;
		}

		return smoothedSum / smoothedCount;
	}

	/**
	 * Returns smoothed standard deviation
	 *
	 * @return smoothed standard deviation
	 */
	@JmxAttribute
	public double getSmoothedStandardDeviation() {
		if (totalCount == 0) {
			return 0.0;
		}

		double avg = smoothedSum / smoothedCount;
		double variance = smoothedSqr / smoothedCount - avg * avg;
		if (variance < 0.0)
			variance = 0.0;
		return sqrt(variance);
	}

	/**
	 * Returns minimum of all added values
	 *
	 * @return minimum of all added values
	 */
	@JmxAttribute(name = "min", optional = true)
	public int getTotalMin() {
		return totalMin;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	@JmxAttribute(name = "max", optional = true)
	public int getTotalMax() {
		return totalMax;
	}

	@JmxAttribute
	public double getAverage() {
		return totalCount != 0L ? totalSum / (double) totalCount : 0.0;
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
		return String.format("%.2f±%.3f   min: %d   max: %d   last: %d   avg: %.2f",
				getSmoothedAverage(), getSmoothedStandardDeviation(), getTotalMin(), getTotalMax(), getLastValue(),
				getAverage());
	}
}
