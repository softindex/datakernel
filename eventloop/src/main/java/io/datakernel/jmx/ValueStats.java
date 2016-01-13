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

import javax.management.openmbean.SimpleType;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.lang.Math.*;

/**
 * Counts added values and computes dynamic average using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class ValueStats implements JmxStats<ValueStats> {

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
	private double smoothedMin;
	private double smoothedMax;

	/**
	 * Resets stats and sets new parameters
	 */
	public void resetStats() {
		smoothedSum = 0.0;
		smoothedSqr = 0.0;
		smoothedCount = 0.0;
		smoothedMin = totalMin;
		smoothedMax = totalMax;
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
	public void refreshStats(long timestamp, double smoothingWindow) {
		if (lastCount == 0)
			return;

		if (lastTimestampMillis == 0L) {
			smoothedSum = lastSum;
			smoothedSqr = lastSqr;
			smoothedCount = lastCount;
			smoothedMin = totalMin = lastMin;
			smoothedMax = totalMax = lastMax;
		} else {
			double windowE = smoothingWindow * 1000.0 / log(2);
			double weight = exp(-(timestamp - lastTimestampMillis) / windowE);

			smoothedSum = lastSum + smoothedSum * weight;
			smoothedSqr = lastSqr + smoothedSqr * weight;
			smoothedCount = lastCount + smoothedCount * weight;

			smoothedMin += (smoothedMax - smoothedMin) * (1.0 - weight);
			smoothedMax += (smoothedMin - smoothedMax) * (1.0 - weight);

			if (lastMin < smoothedMin) {
				smoothedMin = lastMin;
			}

			if (lastMax > smoothedMax) {
				smoothedMax = lastMax;
			}

			totalSum += lastSum;
			totalCount += lastCount;

			if (lastMin < totalMin) {
				totalMin = lastMin;
			}

			if (lastMax > totalMax) {
				totalMax = lastMax;
			}

		}

		lastTimestampMillis = timestamp;

		lastSum = 0;
		lastSqr = 0;
		lastCount = 0;
		lastMin = Integer.MAX_VALUE;
		lastMax = Integer.MIN_VALUE;
	}

	@Override
	public void add(ValueStats counter) {
		if (counter.lastTimestampMillis == 0L)
			return;

		smoothedSum += counter.smoothedSum;
		smoothedSqr += counter.smoothedSqr;
		smoothedCount += counter.smoothedCount;
		if (counter.smoothedMin < smoothedMin) {
			smoothedMin = counter.smoothedMin;
		}
		if (counter.smoothedMax > smoothedMax) {
			smoothedMax = counter.smoothedMax;
		}

		totalSum += counter.totalSum;
		totalCount += counter.totalCount;
		if (counter.totalMin < totalMin) {
			totalMin = counter.totalMin;
		}
		if (counter.totalMax > totalMax) {
			totalMax = counter.totalMax;
		}

		if (counter.lastTimestampMillis > lastTimestampMillis) {
			lastTimestampMillis = counter.lastTimestampMillis;
			lastValue = counter.lastValue;
		}
	}

	@Override
	public SortedMap<String, TypeAndValue> getAttributes() {
		SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
		attributes.put("smoothedAverage", new TypeAndValue(SimpleType.DOUBLE, getSmoothedAverage()));
		attributes.put("smoothedStandardDeviation",
				new TypeAndValue(SimpleType.DOUBLE, getSmoothedStandardDeviation()));
		attributes.put("totalMin", new TypeAndValue(SimpleType.INTEGER, getTotalMin()));
		attributes.put("totalMax", new TypeAndValue(SimpleType.INTEGER, getTotalMax()));
		attributes.put("lastValue", new TypeAndValue(SimpleType.INTEGER, getLastValue()));
		attributes.put("average", new TypeAndValue(SimpleType.DOUBLE, getAverage()));
		attributes.put("smoothedMin", new TypeAndValue(SimpleType.DOUBLE, getSmoothedMin()));
		attributes.put("smoothedMax", new TypeAndValue(SimpleType.DOUBLE, getSmoothedMax()));
		return attributes;
	}

	/**
	 * Returns last added value
	 *
	 * @return last added value
	 */
	public int getLastValue() {
		return lastValue;
	}

	/**
	 * Returns smoothed average of added values
	 *
	 * @return smoothed average of added values
	 */
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
	 * Returns smoothed min of added values
	 *
	 * @return smoothed min of added values
	 */
	public double getSmoothedMin() {
		return smoothedMin;
	}

	/**
	 * Returns smoothed max of added values
	 *
	 * @return smoothed max of added values
	 */
	public double getSmoothedMax() {
		return smoothedMax;
	}

	/**
	 * Returns minimum of all added values
	 *
	 * @return minimum of all added values
	 */
	public int getTotalMin() {
		return totalMin;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	public int getTotalMax() {
		return totalMax;
	}

	public double getAverage() {
		return totalCount != 0L ? totalSum / (double) totalCount : 0.0;
	}

	@Override
	public String toString() {
		return String.format("%.2f±%.3f   min: %d   max: %d   last: %d   avg: %.2f   smoothedMin: %.2f   smoothedMax: %.2f",
				getSmoothedAverage(), getSmoothedStandardDeviation(), getTotalMin(), getTotalMax(), getLastValue(),
				getAverage(), getSmoothedMin(), getSmoothedMax());
	}

}
