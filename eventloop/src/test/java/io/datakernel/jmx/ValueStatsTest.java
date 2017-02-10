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

import org.junit.Test;

import java.util.List;
import java.util.Random;

import static java.lang.Math.sqrt;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ValueStatsTest {
	private static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;
	private static final int ONE_SECOND_IN_MILLIS = 1000;
	private static final Random RANDOM = new Random();

	@Test
	public void smoothedAverageAtLimitShouldBeSameAsInputInCaseOfConstantData() {
		double smoothingWindow = 10.0;
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(smoothingWindow);
		int inputValue = 5;
		int iterations = 1000;
		int refreshPeriod = ONE_SECOND_IN_MILLIS;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += refreshPeriod;
			valueStats.refresh(currentTimestamp);
		}

		double acceptableError = 10E-5;
		assertEquals(inputValue, valueStats.getSmoothedAverage(), acceptableError);
	}

	@Test
	public void itShouldReturnProperStandardDeviationAtLimit() {
		double smoothingWindow = 100.0;
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(smoothingWindow);
		int iterations = 10000;
		int minValue = 0;
		int maxValue = 10;
		int refreshPeriod = 100;

		for (int i = 0; i < iterations; i++) {
			int currentValue = uniformRandom(minValue, maxValue);
			valueStats.recordValue(currentValue);
			currentTimestamp += refreshPeriod;
			valueStats.refresh(currentTimestamp);
		}

		// standard deviation of uniform distribution
		double expectedStandardDeviation = sqrt(((maxValue - minValue + 1) * (maxValue - minValue + 1) - 1) / 12.0);
		double acceptableError = 0.1;
		assertEquals(expectedStandardDeviation, valueStats.getSmoothedStandardDeviation(), acceptableError);
	}

	@Test
	public void itShouldResetStatsAfterResetMethodCall() {
		double smoothingWindow = 10.0;
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(smoothingWindow);
		int inputValue = 5;
		int iterations = 1000;
		int refreshPeriod = ONE_SECOND_IN_MILLIS;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += refreshPeriod;
			valueStats.refresh(currentTimestamp);
		}

		double avgBeforeReset = valueStats.getSmoothedAverage();
		valueStats.resetStats();
		double avgAfterReset = valueStats.getSmoothedAverage();

		double acceptableError = 10E-5;
		assertEquals(inputValue, avgBeforeReset, acceptableError);
		assertEquals(0.0, avgAfterReset, acceptableError);
	}

	@Test
	public void itShouldAccumulateProperly() {
		double smoothingWindow = 10.0;
		long currentTimestamp = 0;
		ValueStats valueStats_1 = ValueStats.create(smoothingWindow);
		ValueStats valueStats_2 = ValueStats.create(smoothingWindow);
		int inputValue_1 = 5;
		int inputValue_2 = 10;
		int iterations = 1000;
		int refreshPeriod = ONE_SECOND_IN_MILLIS;

		for (int i = 0; i < iterations; i++) {
			valueStats_1.recordValue(inputValue_1);
			valueStats_2.recordValue(inputValue_2);
			currentTimestamp += refreshPeriod;
			valueStats_1.refresh(currentTimestamp);
			valueStats_2.refresh(currentTimestamp);
		}

		ValueStats accumulator = new ValueStats();
		accumulator.add(valueStats_1);
		accumulator.add(valueStats_2);

		double acceptableError = 10E-5;
		double expectedAccumulatedSmoothedAvg = (5 + 10) / 2.0;
		assertEquals(expectedAccumulatedSmoothedAvg, accumulator.getSmoothedAverage(), acceptableError);
	}

	@Test
	public void itShouldBuildHistogram() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

		// first interval
		stats.recordValue(2);
		stats.recordValue(3);
		stats.recordValue(-5);
		stats.recordValue(0);

		// second interval
		stats.recordValue(10);
		stats.recordValue(5);
		stats.recordValue(14);

		// no data for third interval

		// fourth interval
		stats.recordValue(600);
		stats.recordValue(1000);

		// first interval
		for (int i = 0; i < 10; i++) {
			stats.recordValue(1);
		}

		List<String> expected = asList(
				"( -∞,   5)  :  14",
				"[  5,  15)  :   3",
				"[ 15, 500)  :   0",
				"[500,  +∞)  :   2"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldNotRenderUnusedLeftAndRightHistogramLevels() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 10, 15, 20, 25, 30, 35});

		stats.recordValue(12);
		stats.recordValue(14);

		stats.recordValue(23);

		List<String> expected = asList(
				"(-∞, 10)  :  0",
				"[10, 15)  :  2",
				"[15, 20)  :  0",
				"[20, 25)  :  1",
				"[25, +∞)  :  0"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneIntermediateValue() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

		stats.recordValue(17);

		List<String> expected = asList(
				"( -∞,  15)  :  0",
				"[ 15, 500)  :  1",
				"[500,  +∞)  :  0"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneRightmostValue() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

		stats.recordValue(600);

		List<String> expected = asList(
				"( -∞, 500)  :  0",
				"[500,  +∞)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneLeftmostValue() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

		stats.recordValue(-10);

		List<String> expected = asList(
				"(-∞,  5)  :  1",
				"[ 5, +∞)  :  0"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldAccumulateHistogram() {
		ValueStats stats_1 = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 10, 15});
		ValueStats stats_2 = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 10, 15});

		// first interval
		stats_1.recordValue(2);
		stats_1.recordValue(4);
		stats_2.recordValue(1);

		// second interval
		stats_1.recordValue(8);

		// no data for third interval

		// fourth interval
		stats_2.recordValue(17);

		stats_1.refresh(1L);
		stats_2.refresh(1L);

		ValueStats accumulator = new ValueStats();
		accumulator.add(stats_1);
		accumulator.add(stats_2);

		List<String> expected = asList(
				"(-∞,  5)  :  3",
				"[ 5, 10)  :  1",
				"[10, 15)  :  0",
				"[15, +∞)  :  1"
		);
		assertEquals(expected, accumulator.getHistogram());
	}

	public static int uniformRandom(int min, int max) {
		return min + (Math.abs(RANDOM.nextInt()) % (max - min + 1));
	}
}
