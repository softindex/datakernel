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

public class EventsCounterTest {

	private static final ManualTimeProvider MANUAL_TIME_PROVIDER = new ManualTimeProvider(0);
	private static final Random RANDOM = new Random();

	@Test
	public void counterShouldUpdatesRateDependingOnPrecision() throws InterruptedException {
		double precision_1_inSeconds = 1.0;
		double precision_2_inSeconds = 2.0;
		int oneSecondInMillis = 1000;
		EventsCounter eventsCounter_1 = new EventsCounter(0.1, precision_1_inSeconds, MANUAL_TIME_PROVIDER);
		EventsCounter eventsCounter_2 = new EventsCounter(0.1, precision_2_inSeconds, MANUAL_TIME_PROVIDER);

		// init time recording
		eventsCounter_1.recordEvents(0);
		eventsCounter_2.recordEvents(0);

		double counter_1_initRate = eventsCounter_1.getSmoothedRate();
		double counter_2_initRate = eventsCounter_2.getSmoothedRate();
		MANUAL_TIME_PROVIDER.upgradeTime(oneSecondInMillis);
		eventsCounter_1.recordEvent();
		eventsCounter_1.recordEvent();
		eventsCounter_2.recordEvent();
		double counter_1_rateAfterUpgrade_1 = eventsCounter_1.getSmoothedRate();
		double counter_2_rateAfterUpgrade_1 = eventsCounter_2.getSmoothedRate();
		MANUAL_TIME_PROVIDER.upgradeTime(oneSecondInMillis);
		eventsCounter_1.recordEvent();
		eventsCounter_2.recordEvent();
		double counter_1_rateAfterUpgrade_2 = eventsCounter_1.getSmoothedRate();
		double counter_2_rateAfterUpgrade_2 = eventsCounter_2.getSmoothedRate();

		double acceptableError = 1E-5;
		assertNotEquals(counter_1_initRate, counter_1_rateAfterUpgrade_1, acceptableError);
		assertEquals(counter_2_initRate, counter_2_rateAfterUpgrade_1, acceptableError); // not enough time passed to be updated
		assertNotEquals(counter_1_rateAfterUpgrade_1, counter_1_rateAfterUpgrade_2, acceptableError);
		assertNotEquals(counter_2_rateAfterUpgrade_1, counter_2_rateAfterUpgrade_2, acceptableError);
	}

	@Test
	public void ifRateIsConstantCounterShouldApproximateThatRateAfterEnoughTimePassed() throws InterruptedException {
		EventsCounter eventsCounter = new EventsCounter(0.1, 0.01, MANUAL_TIME_PROVIDER);
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventsCounter.recordEvent();
			MANUAL_TIME_PROVIDER.upgradeTime(periodInMillis);
//			System.out.println(i + ": " + eventsCounter.getSmoothedRate());
		}

		double acceptableError = 1E-5;
		assertEquals(rate, eventsCounter.getSmoothedRate(), acceptableError);
	}

	@Test
	public void counterShouldResetRateAfterResetMethodCall() {
		EventsCounter eventsCounter = new EventsCounter(0.1, 0.01, MANUAL_TIME_PROVIDER);
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventsCounter.recordEvent();
			MANUAL_TIME_PROVIDER.upgradeTime(periodInMillis);
//			System.out.println(i + ": " + eventsCounter.getSmoothedRate());
		}
		double rateBeforeReset = eventsCounter.getSmoothedRate();
		double initRateOfReset = 0.0;
		eventsCounter.reset();
		double rateAfterReset = eventsCounter.getSmoothedRate();

		double acceptableError = 1E-5;
		assertEquals(rate, rateBeforeReset, acceptableError);
		assertEquals(initRateOfReset, rateAfterReset, acceptableError);
	}

	@Test
	public void counterShouldProperlyCountAllEvents() {
		EventsCounter eventsCounter = new EventsCounter(0.1, 0.01, MANUAL_TIME_PROVIDER);
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventsCounter.recordEvent();
			MANUAL_TIME_PROVIDER.upgradeTime(periodInMillis);
//			System.out.println(i + ": " + eventsCounter.toString());
		}

		assertEquals(events, eventsCounter.getEventsCount());
	}

	@Test
	public void example() {
		EventsCounter eventsCounter = new EventsCounter(1.0, 0.1, MANUAL_TIME_PROVIDER);
		int events = 1000;
		int minPeriod = 100;
		int maxPeriod = 500;

		for (int i = 0; i < events; i++) {
			eventsCounter.recordEvent();
			int periodInMillis = uniformRandom(minPeriod, maxPeriod);
			// after 100 iterations passed, smoothed rate should be increased
			if (i > 100 && i < 200) {
				periodInMillis = 10;
			}
			MANUAL_TIME_PROVIDER.upgradeTime(periodInMillis);
			System.out.println(i + ":   currentPeriodInMillis:  " + periodInMillis + "   eventsStats:   "
					+ eventsCounter.toString());
		}

		assertEquals(events, eventsCounter.getEventsCount());
	}

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
