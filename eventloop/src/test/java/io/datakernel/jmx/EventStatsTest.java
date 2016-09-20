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

import static org.junit.Assert.assertEquals;

public class EventStatsTest {

	private static final Random RANDOM = new Random();

	@Test
	public void ifRateIsConstantEventStatsShouldApproximateThatRateAfterEnoughTimePassed() {
		double smoothingWindow = 1.0;
		EventStats eventStats = EventStats.create().withSmoothingWindow(smoothingWindow);
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}

		double acceptableError = 1E-5;
		assertEquals(rate, eventStats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void counterShouldResetRateAfterResetMethodCall() {
		double smoothingWindow = 1.0;
		EventStats eventStats = EventStats.create().withSmoothingWindow(smoothingWindow);
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			currentTimestamp += periodInMillis;
			eventStats.refresh(currentTimestamp);
		}
		double rateBeforeReset = eventStats.getSmoothedRate();
		eventStats.resetStats();
		double rateAfterReset = eventStats.getSmoothedRate();

		double initRateOfReset = 0.0;
		double acceptableError = 1E-5;
		assertEquals(rate, rateBeforeReset, acceptableError);
		assertEquals(initRateOfReset, rateAfterReset, acceptableError);
	}

	@Test
	public void counterShouldProperlyAggregateEvents() {
		int iterations = 1000;
		final int period = 100;
		long currentTimestamp = 0;

		double smoothingWindow = 10.0;

		EventStats stats_1 = EventStats.create().withSmoothingWindow(smoothingWindow);
		EventStats stats_2 = EventStats.create().withSmoothingWindow(smoothingWindow);
		EventStats stats_3 = EventStats.create().withSmoothingWindow(smoothingWindow);

		for (int i = 0; i < iterations; i++) {
			stats_1.recordEvents(1);
			stats_2.recordEvents(2);
			// we do not record event to stats_3

			currentTimestamp += period;
			stats_1.refresh(currentTimestamp);
			stats_2.refresh(currentTimestamp);
			stats_3.refresh(currentTimestamp);
		}

		EventStats accumulator = EventStats.create();
		accumulator.add(stats_1);
		accumulator.add(stats_2);
		accumulator.add(stats_3);

		double acceptableError = 0.1;
		assertEquals(30.0, accumulator.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldConvergeProperlyWhenNoEventsOccurredBetweenRefreshes() {
		double smoothingWindow = 10.0;
		EventStats stats = EventStats.create().withSmoothingWindow(smoothingWindow);
		int iterations = 1000;
		long currentTimestamp = 0;
		final int period = 200;
		for (int i = 0; i < iterations; i++) {
			// event occurs only once per two refreshes
			int eventsPerRefresh = i % 2;
			stats.recordEvents(eventsPerRefresh);
			currentTimestamp += period;
			stats.refresh(currentTimestamp);
		}

		assertEquals(500, stats.getTotalCount());
		double acceptableError = 0.25;
		assertEquals(2.5, stats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldConvergeProperlyWhenPeriodIsNotStableButPeriodExpectationIsStable() {
		double smoothingWindow = 10.0;
		EventStats stats = EventStats.create().withSmoothingWindow(smoothingWindow);
		int iterations = 1000;
		long currentTimestamp = 0;
		final int period = 200;
		for (int i = 0; i < iterations; i++) {
			stats.recordEvents(1);
			int currentPeriod = period + uniformRandom(-50, 50);
			currentTimestamp += currentPeriod;
			stats.refresh(currentTimestamp);
		}

		assertEquals(1000, stats.getTotalCount());
		double acceptableError = 0.25;
		assertEquals(5.0, stats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldConvergeProperlyWhenAmountOfEventsPerPeriodIsNotStableButExpectationOfAmountOfEventIsStable() {
		double smoothingWindow = 50.0;
		EventStats stats = EventStats.create().withSmoothingWindow(smoothingWindow);
		int iterations = 10000;
		long currentTimestamp = 0;
		final int period = 200;
		for (int i = 0; i < iterations; i++) {
			stats.recordEvents(uniformRandom(0, 10));
			currentTimestamp += period;
			stats.refresh(currentTimestamp);
		}

		double acceptableError = 2.0;
		assertEquals(25.0, stats.getSmoothedRate(), acceptableError);
	}

	public static int uniformRandom(int min, int max) {
		return min + Math.abs(RANDOM.nextInt()) % (max - min + 1);
	}
}
