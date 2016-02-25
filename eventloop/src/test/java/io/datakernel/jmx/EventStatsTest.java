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
		EventStats eventStats = new EventStats();
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refreshStats(currentTimestamp, smoothingWindow);
			currentTimestamp += periodInMillis;
		}

		double acceptableError = 1E-5;
		assertEquals(rate, eventStats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void counterShouldResetRateAfterResetMethodCall() {
		EventStats eventStats = new EventStats();
		double smoothingWindow = 1.0;
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			currentTimestamp += periodInMillis;
			eventStats.refreshStats(currentTimestamp, smoothingWindow);
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
	public void counterShouldProperlyCountAllEvents() {
		EventStats eventStats = new EventStats();
		double smoothingWindow = 1.0;
		long currentTimestamp = 0;
		int events = 100;

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
		}

		eventStats.refreshStats(currentTimestamp, smoothingWindow);

		assertEquals(events, eventStats.getTotalCount());
	}

	@Test
	public void itShouldConvergeProperlyWhenNoEventsOccurredBetweenRefreshes() {
		EventStats stats = new EventStats();
		int iterations = 1000;
		long currentTimestamp = 0;
		final int period = 200;
		for (int i = 0; i < iterations; i++) {
			// event occurs only once per two refreshes
			int eventsPerRefresh = i % 2;
			stats.recordEvents(eventsPerRefresh);
			currentTimestamp += period;
			stats.refreshStats(currentTimestamp, 10.0);
		}

		assertEquals(500, stats.getTotalCount());
		double acceptableError = 0.25;
		assertEquals(2.5, stats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldConvergeProperlyWhenPeriodIsNotStableButPeriodExpectationIsStable() {
		EventStats stats = new EventStats();
		int iterations = 1000;
		long currentTimestamp = 0;
		final int period = 200;
		for (int i = 0; i < iterations; i++) {
			stats.recordEvents(1);
			int currentPeriod = period + uniformRandom(-50, 50);
			currentTimestamp += currentPeriod;
			stats.refreshStats(currentTimestamp, 10.0);
		}

		assertEquals(1000, stats.getTotalCount());
		double acceptableError = 0.25;
		assertEquals(5.0, stats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldConvergeProperlyWhenAmountOfEventsPerPeriodIsNotStableButExpectationOfAmountOfEventIsStable() {
		EventStats stats = new EventStats();
		int iterations = 10000;
		long currentTimestamp = 0;
		final int period = 200;
		for (int i = 0; i < iterations; i++) {
			stats.recordEvents(uniformRandom(0, 10));
			currentTimestamp += period;
			stats.refreshStats(currentTimestamp, 50.0);
		}

		double acceptableError = 2.0;
		assertEquals(25.0, stats.getSmoothedRate(), acceptableError);
	}

	public static int uniformRandom(int min, int max) {
		return min + Math.abs(RANDOM.nextInt()) % (max - min + 1);
	}
}
