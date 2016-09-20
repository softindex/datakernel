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

import java.util.Random;

import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;

public class ValueStatsTest {

	private static final int ONE_SECOND_IN_MILLIS = 1000;
	private static final Random RANDOM = new Random();

	@Test
	public void smoothedAverageAtLimitShouldBeSameAsInputInCaseOfConstantData() {
		double smoothingWindow = 10.0;
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create().withSmoothingWindow(smoothingWindow);
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
		ValueStats valueStats = ValueStats.create().withSmoothingWindow(smoothingWindow);
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
		ValueStats valueStats = ValueStats.create().withSmoothingWindow(smoothingWindow);
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
		ValueStats valueStats_1 = ValueStats.create().withSmoothingWindow(smoothingWindow);
		ValueStats valueStats_2 = ValueStats.create().withSmoothingWindow(smoothingWindow);
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

		ValueStats accumulator = ValueStats.create();
		accumulator.add(valueStats_1);
		accumulator.add(valueStats_2);

		double acceptableError = 10E-5;
		double expectedAccumulatedSmoothedAvg = (5 + 10) / 2.0;
		assertEquals(expectedAccumulatedSmoothedAvg, accumulator.getSmoothedAverage(), acceptableError);
	}

	public static int uniformRandom(int min, int max) {
		return min + (Math.abs(RANDOM.nextInt()) % (max - min + 1));
	}
}
