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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EventsStatsTest {

	private static final Random RANDOM = new Random();

	@Test
	public void ifRateIsConstantEventStatsShouldApproximateThatRateAfterEnoughTimePassed() {
		double smoothingWindow = 1.0;
		EventStats eventStats = new EventStats();
		long currentTimestamp = 0;
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refreshStats(currentTimestamp, smoothingWindow);
			currentTimestamp += periodInMillis;
//			System.out.println(i + ": " + eventStats.getSmoothedRate());
		}

		double acceptableError = 1E-5;
		assertEquals(rate, eventStats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void counterShouldResetRateAfterResetMethodCall() {
		EventStats eventStats = new EventStats();
		double smoothingWindow = 1.0;
		long currentTimestamp = 0;
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			currentTimestamp += periodInMillis;
			eventStats.refreshStats(currentTimestamp, smoothingWindow);
//			System.out.println(i + ": " + eventsCounter.getSmoothedRate());
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
//			System.out.println(i + ": " + eventsCounter.toString());
		}

		eventStats.refreshStats(currentTimestamp, smoothingWindow);

		assertEquals(events, eventStats.getTotalCount());
	}

	@Test
	public void itShouldBehaveProperlyWhenNoEventsOccurredBetweenRefreshes() {
		double smoothingWindow = 0.25;
		EventStats eventStats = new EventStats();
		long currentTimestamp = 0;
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);
		double acceptableError = 1E-2;

		// 1st part
		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			currentTimestamp += periodInMillis;
			eventStats.refreshStats(currentTimestamp, smoothingWindow);
//			System.out.println("1st   " + i + ": " + eventStats.getSmoothedRate());
		}
		assertEquals(rate, eventStats.getSmoothedRate(), acceptableError);

		// 2nd part
		for (int i = 0; i < events; i++) {
			// no events occur between refreshes
			currentTimestamp += periodInMillis;
			eventStats.refreshStats(currentTimestamp, smoothingWindow);
//			System.out.println("2nd   " + i + ": " + eventStats.getSmoothedRate());
		}
		// record one event to enable update
		eventStats.recordEvent();
		currentTimestamp += periodInMillis;
		eventStats.refreshStats(currentTimestamp, smoothingWindow);
		// time_passed = period * number_of_events_in_last_loop
		double almostZeroRate = 1.0 / (period * events);
		assertEquals(almostZeroRate, eventStats.getSmoothedRate(), acceptableError);

		// 3rd part
		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			currentTimestamp += periodInMillis;
			eventStats.refreshStats(currentTimestamp, smoothingWindow);
//			System.out.println("3rd   " + i + ": " + eventStats.getSmoothedRate());
		}
		assertEquals(rate, eventStats.getSmoothedRate(), acceptableError);

	}

//	@Test
//	public void example() {
//		EventStats eventStats = new EventStats();
//		long currentTimestamp = 0;
//		double smoothingWindow = 1.0;
//		int events = 1000;
//		int minPeriod = 100;
//		int maxPeriod = 500;
//
//		for (int i = 0; i < events; i++) {
//			eventStats.recordEvent();
//			int periodInMillis = uniformRandom(minPeriod, maxPeriod);
//			// after 100 iterations passed, smoothed rate should be increased
//			if (i > 100 && i < 200) {
//				periodInMillis = 10;
//			}
//			currentTimestamp += periodInMillis;
//			eventStats.refreshStats(currentTimestamp, smoothingWindow);
//			System.out.println(i + ":   currentPeriodInMillis:  " + periodInMillis + "   eventsStats:   "
//					+ eventStats.toString());
//		}
//	}

	public static int uniformRandom(int min, int max) {
		return min + (Math.abs(RANDOM.nextInt()) % (max + 1));
	}
}
