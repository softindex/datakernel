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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.*;
import static java.util.Arrays.asList;

/**
 * Counts added values and computes dynamic average using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class ValueStats implements JmxRefreshableStats<ValueStats> {
	private static final long TOO_LONG_TIME_PERIOD_BETWEEN_REFRESHES = 5 * 60 * 60 * 1000; // 5 hour
	private static final double LN_2 = log(2);

	// region smoothing window constants
	public static final double SMOOTHING_WINDOW_1_SECOND = 1.0;
	public static final double SMOOTHING_WINDOW_5_SECONDS = 5.0;
	public static final double SMOOTHING_WINDOW_10_SECONDS = 10.0;
	public static final double SMOOTHING_WINDOW_20_SECONDS = 20.0;
	public static final double SMOOTHING_WINDOW_30_SECONDS = 30.0;
	public static final double SMOOTHING_WINDOW_1_MINUTE = 60.0;
	public static final double SMOOTHING_WINDOW_5_MINUTES = 5 * 60.0;
	public static final double SMOOTHING_WINDOW_10_MINUTES = 10 * 60.0;
	public static final double SMOOTHING_WINDOW_20_MINUTES = 20 * 60.0;
	public static final double SMOOTHING_WINDOW_30_MINUTES = 30 * 60.0;
	public static final double SMOOTHING_WINDOW_1_HOUR = 60 * 60.0;
	// endregion

	// region standard levels
	public static final int[] POWERS_OF_TWO =
			new int[]{
					0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072,
					262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728,
					268435456, 536870912, 1073741824
			};

	public static final int[] POWERS_OF_TEN =
			new int[]{
					0, 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
			};

	public static final int[] POWERS_OF_TEN_SHORTENED =
			new int[]{
					0, 1, 10, 100, 1000
			};

	public static final int[] POWERS_OF_TEN_SEMI_LINEAR =
			new int[]{
					0,
					1, 2, 3, 4, 5, 6, 7, 8, 9,
					10, 20, 30, 40, 50, 60, 70, 80, 90,
					100, 200, 300, 400, 500, 600, 700, 800, 900,
					1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
					10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
					100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000,
					1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000,
					10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000,
					100000000, 200000000, 300000000, 400000000, 500000000, 600000000, 700000000, 800000000, 900000000,
					1000000000, 2000000000
			};
	// endregion

	private long lastTimestampMillis;

	// integer runtime accumulators
	private int lastValueInteger;
	private int lastSumInteger;
	private int lastSqrInteger;
	private int lastCountInteger;
	private int lastMinInteger;
	private int lastMaxInteger;

	// double runtime accumulators
	private double lastValueDouble;
	private double lastSumDouble;
	private double lastSqrDouble;
	private int lastCountDouble;
	private double lastMinDouble;
	private double lastMaxDouble;

	// calculated during refresh
	private double totalSum;
	private long totalCount;

	// calculated during refresh
	private double smoothedSum;
	private double smoothedSqr;
	private double smoothedCount;
	private double smoothedMin;
	private double smoothedMax;
	private double smoothedTimeSeconds;
	private double smoothedRate;

	private double smoothingWindow;
	private double smoothingWindowCoef;

	private int[] histogramLevels;
	private long[] histogramValues;

	// fields for aggregation
	private int addedStats;

	// formatting
	private NumberFormatting formatting = NumberFormatting.DECIMAL_TWO_DIGITS;

	// region builders
	private ValueStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
		resetStats();
	}

	private ValueStats() {
		// create accumulator instance, smoothing window will be taken from actual stats
		this.smoothingWindow = -1;
		this.smoothingWindowCoef = -1;
	}

	public static ValueStats createAccumulator() {
		return new ValueStats();
	}

	/**
	 * Creates new ValueStats with specified smoothing window
	 *
	 * @param smoothingWindow in seconds
	 */
	public static ValueStats create(double smoothingWindow) {
		return new ValueStats(smoothingWindow);
	}
	// endregion

	public ValueStats withHistogram(int[] levels) {
		setHistogramLevels(levels);
		return this;
	}

	public ValueStats withNumberFormatting(NumberFormatting formatting) {
		this.formatting = formatting;
		return this;
	}

	public void setHistogramLevels(int[] levels) {
		checkArgument(levels.length > 0, "levels amount must be at least 1");
		for (int i = 1; i < levels.length; i++) {
			checkArgument(levels[i] > levels[i - 1], "levels must be ascending");
		}

		histogramLevels = levels;
		histogramValues = new long[levels.length + 1];
	}

	/**
	 * Resets stats and sets new parameters
	 */
	public void resetStats() {
		smoothedSum = 0.0;
		smoothedSqr = 0.0;
		smoothedCount = 0.0;
		smoothedMin = 0.0;
		smoothedMax = 0.0;

		lastMaxInteger = Integer.MIN_VALUE;
		lastMinInteger = Integer.MAX_VALUE;
		lastSumInteger = 0;
		lastSqrInteger = 0;
		lastCountInteger = 0;
		lastValueInteger = 0;

		lastMaxDouble = -Double.MAX_VALUE;
		lastMinDouble = Double.MAX_VALUE;
		lastSumDouble = 0.0;
		lastSqrDouble = 0.0;
		lastCountDouble = 0;
		lastValueDouble = 0.0;

		lastTimestampMillis = 0L;
		smoothedRate = 0;
		smoothedTimeSeconds = 0;

		totalSum = 0.0;
		totalCount = 0;

		if (histogramLevels != null) {
			for (int i = 0; i < histogramValues.length; i++) {
				histogramValues[i] = 0;
			}
		}
	}

	/**
	 * Adds value
	 */
	public void recordValue(int value) {
		lastValueInteger = value;

		if (value < lastMinInteger) {
			lastMinInteger = value;
		}

		if (value > lastMaxInteger) {
			lastMaxInteger = value;
		}

		lastSumInteger += value;
		lastSqrInteger += value * value;
		lastCountInteger++;

		if (histogramLevels != null) {
			addToHistogram(value);
		}
	}

	public void recordValue(double value) {
		lastValueDouble = value;

		if (value < lastMinDouble) {
			lastMinDouble = value;
		}

		if (value > lastMaxDouble) {
			lastMaxDouble = value;
		}

		lastSumDouble += value;
		lastSqrDouble += value * value;
		lastCountDouble++;
	}

	private void addToHistogram(int value) {
		if (histogramLevels == POWERS_OF_TWO) {
			addToPow2Histogram(value);
		} else {
			if (value >= histogramLevels[histogramLevels.length - 1]) {
				histogramValues[histogramValues.length - 1]++;
			} else {
				int bucketIndex;
				if (histogramLevels.length <= 6) {
					bucketIndex = linearSearch(histogramLevels, value);
				} else {
					bucketIndex = binarySearch(histogramLevels, value);
				}
				histogramValues[bucketIndex]++;
			}
		}
	}

	private void addToPow2Histogram(int value) {
		if (value < 0) {
			histogramValues[0]++;
		} else {
			histogramValues[33 - numberOfLeadingZeros(value)]++;
		}
	}

	// return index of smallest element that is greater than "value"
	private static int linearSearch(int[] histogramLevels, int value) {
		for (int i = 0; i < histogramLevels.length; i++) {
			if (value < histogramLevels[i]) {
				return i;
			}
		}
		return histogramLevels.length; //
	}

	// return index of smallest element that is greater than "value"
	private static int binarySearch(int[] arr, int value) {
		int found = 0;
		int left = 0;
		int right = arr.length - 1;
		while (left < right) {
			if (right - left == 1) {
				found = value < arr[left] ? left : right;
				break;
			}

			int middle = left + (right - left) / 2;
			if (value < arr[middle]) {
				right = middle;
			} else {
				left = middle;
			}
		}
		return found;
	}

	@Override
	public void refresh(long timestamp) {
		double lastSum = 0.0;
		double lastSqr = 0.0;
		long lastCount = 0;

		if (lastCountDouble > 0) {
			lastSum += lastSumDouble;
			lastSqr += lastSqrDouble;
			lastCount += lastCountDouble;
		}

		if (lastCountInteger > 0) {
			lastSum += lastSumInteger;
			lastSqr += lastSqrInteger;
			lastCount += lastCountInteger;
		}

		double lastMin = (lastMinInteger < lastMinDouble) ? lastMinInteger : lastMinDouble;
		double lastMax = (lastMaxInteger > lastMaxDouble) ? lastMaxInteger : lastMaxDouble;

		if (lastTimestampMillis == 0L) {
			smoothedSum = lastSum;
			smoothedSqr = lastSqr;
			smoothedCount = lastCount;
			totalSum = lastSum;
			totalCount = lastCount;
			smoothedMin = lastMin;
			smoothedMax = lastMax;
		} else {
			long timeElapsedMillis = timestamp - lastTimestampMillis;

			if (isTimePeriodValid(timeElapsedMillis)) {
				double timeElapsedSeconds = timeElapsedMillis * 0.001;
				double smoothingFactor = exp(timeElapsedSeconds * smoothingWindowCoef);

				smoothedSum = lastSum + smoothedSum * smoothingFactor;
				smoothedSqr = lastSqr + smoothedSqr * smoothingFactor;
				smoothedCount = lastCount + smoothedCount * smoothingFactor;
				smoothedTimeSeconds = timeElapsedSeconds + smoothedTimeSeconds * smoothingFactor;
				smoothedRate = smoothedCount / smoothedTimeSeconds;

				totalSum += lastSum;
				totalCount += lastCount;

				if (lastCount != 0) {
					smoothedMin += (smoothedMax - smoothedMin) * (1 - smoothingFactor);
					smoothedMax += (smoothedMin - smoothedMax) * (1 - smoothingFactor);

					if (lastMin < smoothedMin) {
						smoothedMin = lastMin;
					}

					if (lastMax > smoothedMax) {
						smoothedMax = lastMax;
					}
				}
			} else {
				// skip stats of last time period
			}
		}

		lastTimestampMillis = timestamp;

		if (lastCountInteger > 0) {
			lastSumInteger = 0;
			lastSqrInteger = 0;
			lastCountInteger = 0;
			lastMinInteger = Integer.MAX_VALUE;
			lastMaxInteger = Integer.MIN_VALUE;
		}

		if (lastCountDouble > 0) {
			lastSumDouble = 0;
			lastSqrDouble = 0;
			lastCountDouble = 0;
			lastMinDouble = Double.MAX_VALUE;
			lastMaxDouble = -Double.MAX_VALUE;
		}
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
		smoothedRate += anotherStats.smoothedRate;

		totalSum += anotherStats.totalSum;
		totalCount += anotherStats.totalCount;

		if (addedStats == 0) {
			smoothedMin = anotherStats.smoothedMin;
			smoothedMax = anotherStats.smoothedMax;
		} else {
			if (anotherStats.smoothedMin < smoothedMin) {
				smoothedMin = anotherStats.smoothedMin;
			}
			if (anotherStats.smoothedMax > smoothedMax) {
				smoothedMax = anotherStats.smoothedMax;
			}
		}

		if (anotherStats.lastTimestampMillis > lastTimestampMillis) {
			lastTimestampMillis = anotherStats.lastTimestampMillis;
			lastValueInteger = anotherStats.lastValueInteger;
		}

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

		// histogram
		if (addedStats == 0) {
			if (anotherStats.histogramLevels != null) {
				this.histogramLevels = Arrays.copyOf(anotherStats.histogramLevels, anotherStats.histogramLevels.length);
				this.histogramValues = Arrays.copyOf(anotherStats.histogramValues, anotherStats.histogramValues.length);
			}
		} else {
			if (this.histogramLevels != null) {
				for (int i = 0; i < histogramValues.length; i++) {
					histogramValues[i] += anotherStats.histogramValues[i];
				}
			}
		}

		addedStats++;
	}

	private static double calculateSmoothingWindowCoef(double smoothingWindow) {
		return -(LN_2 / smoothingWindow);
	}

	/**
	 * Returns last added value
	 *
	 * @return last added value
	 */
	@JmxAttribute(optional = true)
	public double getLastValue() {
		return (lastCountInteger > lastCountDouble) ? lastValueInteger : lastValueDouble;
	}

	/**
	 * Returns smoothed average of added values
	 *
	 * @return smoothed average of added values
	 */
	@JmxAttribute(optional = true)
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
	@JmxAttribute(optional = true)
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
	public double getSmoothedMin() {
		return totalCount == 0 ? 0.0 : smoothedMin;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	@JmxAttribute(name = "max", optional = true)
	public double getSmoothedMax() {
		return totalCount == 0 ? 0.0 : smoothedMax;
	}

	@JmxAttribute(optional = true)
	public double getAverage() {
		return totalCount != 0L ? totalSum / totalCount : 0.0;
	}

	@JmxAttribute(optional = true)
	public double getSmoothedRate() {
		return smoothedRate;
	}

	@JmxAttribute(optional = true)
	public double getSmoothingWindow() {
		return smoothingWindow;
	}

	@JmxAttribute
	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
	}

	@JmxAttribute(optional = true)
	public long getCount() {
		return totalCount;
	}

	@JmxAttribute(optional = true)
	public List<String> getHistogram() {
		if (histogramLevels == null) {
			return null;
		}

		if (!histogramContainsValues()) {
			return null;
		}

		int left = findLeftHistogramLimit();
		int right = findRightHistogramLimit();

		String[] lines = new String[right - left + 1];
		String[] labels = createHistogramLabels(histogramLevels, left, right - 1);
		long[] values = Arrays.copyOfRange(histogramValues, left, right + 1);

		int maxValueStrLen = 0;
		for (long value : histogramValues) {
			String valueStr = Long.toString(value);
			if (valueStr.length() > maxValueStrLen) {
				maxValueStrLen = valueStr.length();
			}
		}

		String pattern = "  :  %" + maxValueStrLen + "s";
		for (int i = 0; i < values.length; i++) {
			lines[i] = labels[i] + String.format(pattern, values[i]);
		}

		return asList(lines);
	}

	private boolean histogramContainsValues() {
		if (histogramValues == null) {
			return false;
		}

		for (long value : histogramValues) {
			if (value != 0) {
				return true;
			}
		}

		return false;
	}

	private int findLeftHistogramLimit() {
		int left = 0;
		for (int i = 0; i < histogramValues.length; i++) {
			if (histogramValues[i] != 0) {
				left = i;
				break;
			}
		}
		left = left > 0 ? left - 1 : left;
		return left;
	}

	private int findRightHistogramLimit() {
		int right = histogramValues.length - 1;
		for (int i = histogramValues.length - 1; i >= 0; i--) {
			if (histogramValues[i] != 0) {
				right = i;
				break;
			}
		}
		right = right < histogramValues.length - 1 ? right + 1 : right;
		return right;
	}

	private static String[] createHistogramLabels(int[] levels, int left, int right) {
		int maxLevelStrLen = 0;
		for (int i = left; i <= right; i++) {
			String levelStr = Integer.toString(levels[i]);
			if (levelStr.length() > maxLevelStrLen) {
				maxLevelStrLen = levelStr.length();
			}
		}

		String negInf = "-∞";
		String posInf = "+∞";

		int maxLeftSymbols = Math.max(negInf.length(), maxLevelStrLen);
		int maxRightSymbols = Math.max(posInf.length(), maxLevelStrLen);

		String pattern = "%" + maxLeftSymbols + "s, %" + maxRightSymbols + "s";

		List<String> labels = new ArrayList<>(right - left + 1 + 2);
		labels.add("(" + String.format(pattern, negInf, levels[left]) + ")");
		for (int i = left + 1; i <= right; i++) {
			labels.add("[" + String.format(pattern, levels[i - 1], levels[i]) + ")");
		}
		labels.add("[" + String.format(pattern, levels[right], posInf) + ")");

		return labels.toArray(new String[labels.size()]);
	}

	@JmxAttribute
	public String get() {
		return toString();
	}

	@Override
	public String toString() {
		String rawTemplate = "{format1}±{format2} [{format1}...{format1}]  last: {format1}  values: %d @ {format2}/s";
		String template;
		switch (formatting) {
			case AUTO:
				template = rawTemplate.replace("{format1}", "%.2g").replace("{format2}", "%.3g");
				break;
			case SCIENTIFIC_NOTATION:
				template = rawTemplate.replace("{format1}", "%.2e").replace("{format2}", "%.3e");
				break;
			case DECIMAL_TWO_DIGITS:
				template = rawTemplate.replace("{format1}", "%.2f").replace("{format2}", "%.3f");
				break;
			default:
				template = rawTemplate.replace("{format1}", "%.2f").replace("{format2}", "%.3f");
		}
		return String.format(template, getSmoothedAverage(), getSmoothedStandardDeviation(), getSmoothedMin(),
				getSmoothedMax(), getLastValue(), getCount(), getSmoothedRate());
	}

	// region helper classes
	public enum NumberFormatting {
		AUTO, SCIENTIFIC_NOTATION, DECIMAL_TWO_DIGITS
	}
	// endregion
}
