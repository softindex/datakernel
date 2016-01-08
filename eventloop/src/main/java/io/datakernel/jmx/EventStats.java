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

import java.util.SortedMap;

import static java.lang.Math.exp;
import static java.lang.Math.log;

/**
 * Computes total amount of events and dynamic rate using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class EventStats implements JmxStats<EventStats> {
	private boolean computationStarted;
	private long lastTimestampMillis;
	private int lastCount;

	private long totalCount;

	//	private double smoothedTime;
//	private double smoothedCount;
	private double smoothedPeriod;

//	private int addedEventStatsCount;

	/**
	 * Creates {@link EventStats} with specified parameters and rate = 0
	 */
	public EventStats() {
	}

	/**
	 * Resets rate to zero
	 */
	public void resetStats() {
		lastCount = 0;
//		smoothedTime = 0;
		totalCount = 0;
		lastTimestampMillis = 0;
		smoothedPeriod = 0;
		computationStarted = false;
//		addedEventStatsCount = 0;
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
	}

	@Override
	public void refreshStats(long timestamp, double smoothingWindow) {
		// TODO(vmykhalko): refactor this method (tests are already written)
		if (computationStarted) {
			if (lastCount > 0) {
				long timeElapsedMillis = timestamp - lastTimestampMillis;

				double windowE = smoothingWindow * 1000.0 / log(2);
				double weight = exp(-timeElapsedMillis / windowE);

				smoothedPeriod = (1 - weight) * (timeElapsedMillis / (double) lastCount) + smoothedPeriod * weight;

				lastTimestampMillis = timestamp;
				totalCount += lastCount;
				lastCount = 0;
			} else {
				// ignore update
			}
		} else {
			if (lastTimestampMillis != 0L) {
				if (lastCount > 0) {
					smoothedPeriod = (timestamp - lastTimestampMillis) / (double) lastCount;
					lastTimestampMillis = timestamp;
					totalCount += lastCount;
					lastCount = 0;
					computationStarted = true;
				} else {
					// ignore update
				}
			}
			lastTimestampMillis = timestamp;
			totalCount += lastCount;
			lastCount = 0;
		}
	}

	@Override
	public SortedMap<String, TypeAndValue> getAttributes() {
		throw new UnsupportedOperationException(); // TODO (vmykhalko)
	}

	@Override

	public void add(EventStats anotherStats) {
		// TODO(vmykhalko): revisit / test this code
		totalCount += anotherStats.totalCount;
		smoothedPeriod = (1.0 / (getSmoothedRate() + anotherStats.getSmoothedRate())) * 1000;
		computationStarted = true;
	}

	/**
	 * Returns smoothed value of rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed value of rate in events per second
	 */
	public double getSmoothedRate() {
		return computationStarted ? 1.0 / (smoothedPeriod / 1000.0) : 0.0;
	}

	/**
	 * Returns total amount of recorded events
	 *
	 * @return total amount of recorded events
	 */
	public long getTotalCount() {
		return totalCount;
	}

	@Override
	public String toString() {
		return String.format("total: %d   smoothedRate: %.3f",
				getTotalCount(), getSmoothedRate());
	}
}
