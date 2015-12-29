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

import io.datakernel.time.CurrentTimeProvider;

import static java.lang.Math.*;

/**
 * Counts added values and computes dynamic average using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class ValuesCounter {

	private static final double ONE_SECOND_IN_MILLIS = 1000.0;
	private static final double DEFAULT_INITIAL_DYNAMIC_AVG = 0.0;
	private static final double DEFAULT_INITIAL_DYNAMIC_VARIANCE = 0.0;

	private final CurrentTimeProvider timeProvider;
	private double windowE;
	private double precision;
	private long lastTimestampMillis;
	private int lastValuesSum;
	private int lastValuesAmount;
	private int lastValue;
	private int maxValue;
	private int minValue;
	private int maxValueAtLastStep;
	private int minValueAtLastStep;
	private double smoothedAverage;
	private double smoothedVariance;
	private double smoothedMin;
	private double smoothedMax;

	/**
	 * Creates {@link ValuesCounter} with specified parameters
	 *
	 * @param window       time in seconds at which weight of appropriate value is 0.5
	 * @param precision    time in seconds to update dynamic average
	 * @param timeProvider provider of current time
	 */
	public ValuesCounter(double window, double precision, CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets stats and sets new parameters
	 *
	 * @param window    time in seconds at which weight of appropriate value is 0.5
	 * @param precision time in seconds to update dynamic average
	 */
	public void reset(double window, double precision) {
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets stats and sets initial value to zero
	 */
	public void reset() {
		resetValues(windowE, precision);
	}

	private void resetValues(double windowE, double precisionInMillis) {
		this.windowE = windowE;
		this.precision = precisionInMillis;
		this.smoothedAverage = DEFAULT_INITIAL_DYNAMIC_AVG;
		this.smoothedVariance = DEFAULT_INITIAL_DYNAMIC_VARIANCE;
		this.lastTimestampMillis = timeProvider.currentTimeMillis();
		this.lastValuesSum = 0;
		this.lastValuesAmount = 0;
		this.lastValue = 0;
		this.maxValue = Integer.MIN_VALUE;
		this.minValue = Integer.MAX_VALUE;
		this.maxValueAtLastStep = this.maxValue;
		this.minValueAtLastStep = this.minValue;
		this.smoothedMax = this.maxValue;
		this.smoothedMin = this.minValue;
	}

	/**
	 * Adds value
	 */
	public void recordValue(int value) {
		lastValue = value;
		updateMax(value);
		updateMin(value);

		int timeElapsedMillis = (int) (timeProvider.currentTimeMillis() - lastTimestampMillis);
		lastValuesSum += value;
		++lastValuesAmount;
		if (timeElapsedMillis >= precision) {
			performComputations(timeElapsedMillis);
		}
	}

	private void updateMax(int value) {
		if (value > maxValueAtLastStep) {
			maxValueAtLastStep = value;
			if (value > maxValue) {
				maxValue = value;
			}
		}
	}

	private void updateMin(int value) {
		if (value < minValueAtLastStep) {
			minValueAtLastStep = value;
			if (value < minValue) {
				minValue = value;
			}
		}
	}

	private void performComputations(int timeElapsedMillis) {
		double weight = 1 - exp(-timeElapsedMillis / windowE);

		updateSmoothedAverageAndDeviation(weight);
		updateSmoothedMin(weight);
		updateSmoothedMax(weight);

		lastTimestampMillis += timeElapsedMillis;
		lastValuesSum = 0;
		lastValuesAmount = 0;
		minValueAtLastStep = Integer.MAX_VALUE;
		maxValueAtLastStep = Integer.MIN_VALUE;
	}

	private void updateSmoothedAverageAndDeviation(double weight) {
		double lastValuesAvg = lastValuesSum / (double) lastValuesAmount;
		smoothedAverage += (lastValuesAvg - smoothedAverage) * weight;
		double currentDeviationSquared = pow((smoothedAverage - lastValuesAvg), 2.0);
		smoothedVariance += (currentDeviationSquared - smoothedVariance) * weight;
	}

	private void updateSmoothedMin(double weight) {
		if (minValueAtLastStep < smoothedMin) {
			smoothedMin = minValueAtLastStep;
		} else {
			smoothedMin += (smoothedMax - smoothedMin) * weight;
		}
	}

	private void updateSmoothedMax(double weight) {
		if (maxValueAtLastStep > smoothedMax) {
			smoothedMax = maxValueAtLastStep;
		} else {
			smoothedMax += (smoothedMin - smoothedMax) * weight;
		}
	}

	/**
	 * Returns dynamic average of added values
	 *
	 * @return dynamic average of added values
	 */
	public double getSmoothedAverage() {
		return smoothedAverage;
	}

	/**
	 * Returns dynamic standard deviation
	 *
	 * @return dynamic standard deviation
	 */
	public double getSmoothedStandardDeviation() {
		return sqrt(smoothedVariance);
	}

	/**
	 * Returns minimum of all added values
	 *
	 * @return minimum of all added values
	 */
	public int getMinValue() {
		return minValue;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	public int getMaxValue() {
		return maxValue;
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
	 * Returns dynamic max of added values
	 *
	 * @return dynamic max of added values
	 */
	public double getSmoothedMax() {
		return smoothedMax;
	}

	/**
	 * Returns dynamic min of added values
	 *
	 * @return dynamic min of added values
	 */
	public double getSmoothedMin() {
		return smoothedMin;
	}

	@Override
	public String toString() {
		return String.format("%.2f±%.3f   min: %d   max: %d   last: %d   smoothedMin: %.2f   smoothedMax: %.2f",
				getSmoothedAverage(), getSmoothedStandardDeviation(), getMinValue(), getMaxValue(), getLastValue(),
				getSmoothedMin(), getSmoothedMax());
	}

	private static double secondsToMillis(double precisionInSeconds) {
		return precisionInSeconds * ONE_SECOND_IN_MILLIS;
	}

	private static double transformWindow(double windowBase2InSeconds) {
		return windowBase2InSeconds * ONE_SECOND_IN_MILLIS / log(2);
	}

	public static Accumulator accumulator() {
		return new Accumulator();
	}

	public static final class Accumulator {

		private double totalSmoothedAverage;
		private double totalSmoothedVariance;
		private int minValue;
		private int maxValue;
		private int totalCounters;

		private Accumulator() {
			this.totalSmoothedAverage = 0.0;
			this.totalSmoothedVariance = 0.0;
			this.minValue = 0;
			this.maxValue = 0;
			this.totalCounters = 0;
		}

		public void add(ValuesCounter counter) {
			if (counter.minValue < this.minValue) {
				this.minValue = counter.minValue;
			}
			if (counter.maxValue > this.maxValue) {
				this.maxValue = counter.maxValue;
			}
			this.totalSmoothedAverage += counter.smoothedAverage;
			this.totalSmoothedVariance += counter.smoothedVariance;
			this.totalCounters++;
		}

		public double getSmoothedAverage() {
			return totalSmoothedAverage / totalCounters;
		}

		public double getSmoothedStandardDeviation() {
			return sqrt(totalSmoothedVariance / totalCounters);
		}

		public int getMinValue() {
			return minValue;
		}

		public int getMaxValue() {
			return maxValue;
		}

		@Override
		public String toString() {
			return String.format("%.2f±%.3f   min: %d   max: %d",
					getSmoothedAverage(), getSmoothedStandardDeviation(), getMinValue(), getMaxValue());
		}
	}
}
