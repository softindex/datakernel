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
import org.junit.Test;

import java.util.Random;

import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;

public class ValuesCounterTest {

	private static final ManualTimeProvider MANUAL_TIME_PROVIDER = new ManualTimeProvider(0);
	private static final int ONE_SECOND_IN_MILLIS = 1000;
	private static final Random RANDOM = new Random();

	@Test
	public void dynamicAverageAtLimitShouldBeSameAsInputInCaseOfConstantData() {
		double window = 10.0;
		double precision = 0.1;
		ValuesCounter valuesCounter = new ValuesCounter(window, precision, MANUAL_TIME_PROVIDER);
		int inputValue = 5;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			MANUAL_TIME_PROVIDER.upgradeTime(ONE_SECOND_IN_MILLIS);
			valuesCounter.recordValue(inputValue);
		}

		double acceptableError = 10E-5;
		assertEquals(inputValue, valuesCounter.getSmoothedAverage(), acceptableError);
	}

	@Test
	public void itShouldReturnProperStandardDeviationAtLimit() {
		double window = 100.0;
		double precision = 0.1;
		ValuesCounter counter = new ValuesCounter(window, precision, MANUAL_TIME_PROVIDER);
		int iterations = 10000;
		int minValue = 0;
		int maxValue = 10;

		for (int i = 0; i < iterations; i++) {
			MANUAL_TIME_PROVIDER.upgradeTime(100);
			int currentValue = uniformRandom(minValue, maxValue);
			counter.recordValue(currentValue);
		}

		// standard deviation of uniform distribution
		double expectedStandardDeviation = sqrt(((maxValue - minValue + 1) * (maxValue - minValue + 1) - 1) / 12.0);
		double acceptableError = 0.1;
		assertEquals(expectedStandardDeviation, counter.getSmoothedStandardDeviation(), acceptableError);
	}

	@Test
	public void itShouldResetStatsAfterResetMethodCall() {
		double window = 10.0;
		double precision = 0.1;
		ValuesCounter valuesCounter = new ValuesCounter(window, precision, MANUAL_TIME_PROVIDER);
		int inputValue = 5;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			MANUAL_TIME_PROVIDER.upgradeTime(ONE_SECOND_IN_MILLIS);
			valuesCounter.recordValue(inputValue);
		}

		double avgBeforeReset = valuesCounter.getSmoothedAverage();
		valuesCounter.reset();
		double avgAfterReset = valuesCounter.getSmoothedAverage();

		double acceptableError = 10E-5;
		assertEquals(inputValue, avgBeforeReset, acceptableError);
		assertEquals(0.0, avgAfterReset, acceptableError);
	}

//	@Test
//	public void example() {
//		double window = 10.0;
//		double precision = 0.001;
//		ValuesCounter counter = new ValuesCounter(window, precision, MANUAL_TIME_PROVIDER);
//		int iterations = 1000;
//		int minValue = 0;
//		int maxValue = 500;
//
//		for (int i = 0; i < iterations; i++) {
//			MANUAL_TIME_PROVIDER.upgradeTime(100);
//			int currentValue = uniformRandom(minValue, maxValue);
//			counter.recordValue(currentValue);
//			System.out.println(i + ":   stats: " + counter);
//		}
//	}

	public static final class ManualTimeProvider implements CurrentTimeProvider {

		private long currentTime;

		public ManualTimeProvider(long currentTime) {
			this.currentTime = currentTime;
		}

		public void upgradeTime(int millis) {
			currentTime += millis;
		}

		@Override
		public long currentTimeMillis() {
			return currentTime;
		}
	}

	public static int uniformRandom(int min, int max) {
		return min + (Math.abs(RANDOM.nextInt()) % (max + 1));
	}
}
