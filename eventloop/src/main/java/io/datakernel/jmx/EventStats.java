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

import static java.lang.Math.pow;

/**
 * Computes total amount of events and dynamic rate using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class EventStats implements JmxStats<EventStats> {
	private static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private long lastTimestampMillis;
	private int lastCount;

	private long totalCount;
	private double smoothedCount;
	private double smoothedRate;

	private double smoothingWindow;

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
	public void refreshStats(long timestamp) {
		long timeElapsedMillis = timestamp - lastTimestampMillis;
		double timeElapsedSeconds = timeElapsedMillis / 1000.0;
		smoothedCount = lastCount + smoothedCount * pow(2.0, -(timeElapsedSeconds / smoothingWindow));

		if (timeElapsedMillis != 0) {
			double adjustmentCoef = computeAdjustmentCoef(smoothingWindow, timeElapsedSeconds);
			smoothedRate = smoothedCount * adjustmentCoef;
		} else {
			smoothedRate = 0;
		}

		lastCount = 0;
		lastTimestampMillis = timestamp;
	}

	private static double computeAdjustmentCoef(double smoothingWindow, double timeElapsedSeconds) {
		double ratio = smoothingWindow / timeElapsedSeconds;
		double coef = -0.5 * (pow(2.0, (ratio - 1) / ratio) - 2);
		coef *= 1 / timeElapsedSeconds;
		return coef;
	}

	@Override
	public void add(EventStats anotherStats) {
		totalCount += anotherStats.totalCount;
		smoothedCount += anotherStats.smoothedCount;
		smoothedRate += anotherStats.smoothedRate;
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

	@JmxAttribute
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
