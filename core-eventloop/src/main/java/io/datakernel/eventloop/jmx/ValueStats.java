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

package io.datakernel.eventloop.jmx;

import io.datakernel.jmx.api.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.common.Preconditions.checkArgument;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.*;
import static java.util.Arrays.asList;

/**
 * Counts added values and computes dynamic average using exponential smoothing algorithm
 * <p>
 * Class is supposed to work in single thread
 */
public final class ValueStats implements JmxRefreshableStats<ValueStats>, JmxStatsWithReset, JmxStatsWithSmoothingWindow {
	private static final long TOO_LONG_TIME_PERIOD_BETWEEN_REFRESHES = Duration.ofHours(1).toMillis();
	private static final double LN_2 = log(2);

	// region standard levels
	public static final int[] POWERS_OF_TWO =
			{
					0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072,
					262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728,
					268435456, 536870912, 1073741824
			};

	public static final int[] POWERS_OF_TEN =
			{
					0, 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
			};

	public static final int[] POWERS_OF_TEN_SHORTENED =
			{
					0, 1, 10, 100, 1000
			};

	public static final int[] POWERS_OF_TEN_SEMI_LINEAR =
			{
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

	public static final int[] POWERS_OF_TEN_SEMI_LINEAR_1000 =
			{
					0,
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
	private double absoluteMaxValue;
	private double absoluteMinValue;
	private double smoothedTimeSeconds;
	private double smoothedRate;

	private double smoothingWindow;
	private double smoothingWindowCoef;

	private int[] histogramLevels;
	private long[] histogramValues;

	// fields for aggregation
	private int addedStats;

	// formatting
	@Nullable
	private String unit;
	@Nullable
	private String rateUnit;
	private boolean useAvgAndDeviaton = true;
	private boolean useMinMax = true;
	private boolean useLastValue = true;
	private boolean useAbsoluteValues;
	private int precision = 1000;

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
	public static ValueStats create(Duration smoothingWindow) {
		return new ValueStats(smoothingWindow.toMillis() / 1000.0);
	}

	public ValueStats withUnit(String unit) {
		this.unit = unit;
		return this;
	}

	public ValueStats withRate(String rateUnit) {
		this.rateUnit = rateUnit;
		return this;
	}

	public ValueStats withRate() {
		this.rateUnit = "";
		return this;
	}

	public ValueStats withHistogram(int[] levels) {
		setHistogramLevels(levels);
		return this;
	}

	public ValueStats withAbsoluteValues(boolean value) {
		this.useAbsoluteValues = value;
		return this;
	}

	public ValueStats withAverageAndDeviation(boolean value) {
		this.useAvgAndDeviaton = value;
		return this;
	}

	public ValueStats withMinMax(boolean value) {
		this.useMinMax = value;
		return this;
	}

	public ValueStats withLastValue(boolean value) {
		this.useLastValue = value;
		return this;
	}

	public ValueStats withPrecision(int precision) {
		checkArgument(precision > 0, "Precision should be a positive value");
		this.precision = precision;
		return this;
	}

	public ValueStats withScientificNotation() {
		this.precision = -1;
		return this;
	}

	// endregion

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
	@Override
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
			Arrays.fill(histogramValues, 0);
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

		absoluteMinValue = min(absoluteMinValue, lastMin);
		absoluteMaxValue = max(absoluteMaxValue, lastMax);

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
			lastValueDouble = anotherStats.lastValueDouble;
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
				histogramLevels = Arrays.copyOf(anotherStats.histogramLevels, anotherStats.histogramLevels.length);
				histogramValues = Arrays.copyOf(anotherStats.histogramValues, anotherStats.histogramValues.length);
			}
		} else {
			if (histogramLevels != null) {
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
	 * Returns smoothed minimum of all added values
	 *
	 * @return smoothed minimum of all added values
	 */
	@JmxAttribute(name = "min", optional = true)
	public double getSmoothedMin() {
		return totalCount == 0 ? 0.0 : smoothedMin;
	}

	/**
	 * Returns smoothed maximum of all added values
	 *
	 * @return smoothed maximum of all added values
	 */
	@JmxAttribute(name = "max", optional = true)
	public double getSmoothedMax() {
		return totalCount == 0 ? 0.0 : smoothedMax;
	}

	/**
	 * Returns minimum of all added values
	 *
	 * @return minimum of all added values
	 */
	@JmxAttribute(name = "absoluteMin", optional = true)
	public double getAbsosuteMin() {
		return totalCount == 0 ? 0.0 : absoluteMinValue;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	@JmxAttribute(name = "absoluteMax", optional = true)
	public double getAbsoluteMax() {
		return totalCount == 0 ? 0.0 : absoluteMaxValue;
	}

	@JmxAttribute(optional = true)
	public double getAverage() {
		return totalCount != 0L ? totalSum / totalCount : 0.0;
	}

	@JmxAttribute(optional = true)
	public double getSmoothedRate() {
		return smoothedRate;
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

	@JmxAttribute(optional = true)
	public long getCount() {
		return totalCount;
	}

	@Nullable
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

		int maxLeftSymbols = max(negInf.length(), maxLevelStrLen);
		int maxRightSymbols = max(posInf.length(), maxLevelStrLen);

		String pattern = "%" + maxLeftSymbols + "s, %" + maxRightSymbols + "s";

		List<String> labels = new ArrayList<>(right - left + 1 + 2);
		labels.add("(" + String.format(pattern, negInf, levels[left]) + ")");
		for (int i = left + 1; i <= right; i++) {
			labels.add("[" + String.format(pattern, levels[i - 1], levels[i]) + ")");
		}
		labels.add("[" + String.format(pattern, levels[right], posInf) + ")");

		return labels.toArray(new String[0]);
	}

	@JmxAttribute
	public String get() {
		return toString();
	}

	@Override
	public String toString() {
		if (totalCount == 0) {
			return "<totalCount is 0>";
		}

		double min = smoothedMin;
		double max = smoothedMax;
		DecimalFormat decimalFormat;

		if (useAbsoluteValues) {
			min = absoluteMinValue;
			max = absoluteMaxValue;
		}

		if (precision == -1) {
			decimalFormat = new DecimalFormat("0.0####E0#");
		} else {
			decimalFormat = new DecimalFormat("0");
			decimalFormat.setMaximumFractionDigits((int) ceil(min(max(-log10(abs(max - min) / precision), 0), 6)));
		}

		StringBuilder constructorTemplate = new StringBuilder();

		// average and deviation
		if (useAvgAndDeviaton) {
			constructorTemplate
					.append(decimalFormat.format(getSmoothedAverage()))
					.append('±')
					.append(decimalFormat.format(getSmoothedStandardDeviation()))
					.append(' ');
			if (unit != null) {
				constructorTemplate.append(unit)
						.append("  ");
			} else {
				constructorTemplate.append(' ');
			}
		}

		// min and max
		if (useMinMax) {
			constructorTemplate
					.append('[')
					.append(decimalFormat.format(min))
					.append("...")
					.append(decimalFormat.format(max))
					.append("]  ");
		}

		// last value
		if (useLastValue) {
			constructorTemplate
					.append("last: ")
					.append(decimalFormat.format(getLastValue()))
					.append("  ");
		}

		// rate
		if (rateUnit != null) {
			constructorTemplate
					.append("calls: ")
					.append(EventStats.format(totalCount, smoothedRate, rateUnit, decimalFormat));
		}

		return constructorTemplate.toString().trim();
	}
}
