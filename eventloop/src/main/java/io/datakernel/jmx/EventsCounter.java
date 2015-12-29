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

import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Math.exp;
import static java.lang.Math.log;

/**
 * Computes total amount of events and dynamic rate using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class EventsCounter {
	private static final double ONE_SECOND_IN_MILLIS = 1000.0;
//	private static final int MIN_PRECISION_IN_MILLIS = 1;
//	private static final int MAX_PRECISION_IM_MILLIS = Integer.MAX_VALUE;

	private final CurrentTimeProvider timeProvider;
	private double windowE;
	private int precision;
	private long lastTimestampMillis;
	private int eventPerLastTimePeriod;
	private double smoothedPeriod;
	private long totalEvents;
	private double smoothedMinRate;
	private double smoothedMaxRate;
	private boolean firstIterationCompleted;
	private boolean recordEventCalledAtLeastOnce;

	/**
	 * Creates {@link EventsCounter} with specified parameters and rate = 0
	 *
	 * @param window       time in seconds at which weight of appropriate rate is 0.5
	 * @param precision    time in seconds to update dynamic rate
	 * @param timeProvider provider of current time
	 */
	public EventsCounter(double window, double precision, CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets rate to zero and sets new parameters
	 *
	 * @param window    time in seconds at which weight of appropriate rate is 0.5
	 * @param precision time in seconds to update dynamic rate
	 */
	public void reset(double window, double precision) {
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets rate to zero
	 */
	public void reset() {
		resetValues(windowE, precision);
	}

	private void resetValues(double windowE, double precisionInMillis) {
//		checkArgument(precisionInMillis >= MIN_PRECISION_IN_MILLIS && precisionInMillis <= MAX_PRECISION_IM_MILLIS,
//				"Smoothing precision must be in range [%d, %d] milliseconds",
//				MIN_PRECISION_IN_MILLIS, MAX_PRECISION_IM_MILLIS);

		this.windowE = windowE;
		this.precision = (int) precisionInMillis;
		this.eventPerLastTimePeriod = 0;
		this.smoothedPeriod = 0;
		this.totalEvents = 0;
		this.smoothedMinRate = Double.MAX_VALUE;
		this.smoothedMaxRate = Double.MIN_VALUE;
		this.firstIterationCompleted = false;
		this.recordEventCalledAtLeastOnce = false;
		// we don't initialize lastTimestampMillis here
	}

	/**
	 * Records event and updates rate
	 */
	public void recordEvent() {
		recordEvents(1);
	}

	/**
	 * Records events and updates rate
	 *
	 * @param events number of events
	 */
	public void recordEvents(int events) {
		checkArgument(events >= 0, "Number of events cannot be negative");

		// timestamp should be initialized after first recordEvent() call to avoid "cold start problem"
		if (!recordEventCalledAtLeastOnce) {
			lastTimestampMillis = timeProvider.currentTimeMillis();
			recordEventCalledAtLeastOnce = true;
		}

		int timeElapsedMillis = (int) (timeProvider.currentTimeMillis() - lastTimestampMillis);
		eventPerLastTimePeriod += events;
		totalEvents += events;
		if (timeElapsedMillis >= precision) {
			if (eventPerLastTimePeriod > 0) {
				double lastPeriodAvg = (double) (timeElapsedMillis) / eventPerLastTimePeriod;

				if (!firstIterationCompleted) {
					smoothedPeriod = lastPeriodAvg;
					firstIterationCompleted = true;
				} else {
					double weight = 1 - exp(-timeElapsedMillis / windowE);
					smoothedPeriod += (lastPeriodAvg - smoothedPeriod) * weight;
					updateSmoothedMin(weight);
					updateSmoothedMax(weight);
				}

				lastTimestampMillis += timeElapsedMillis;
				eventPerLastTimePeriod = 0;
			} else {
				// we don't update last timestamp
			}
		}
	}

	private void updateSmoothedMax(double weight) {
		double smoothedRate = getSmoothedRate();
		if (smoothedRate > smoothedMaxRate) {
			smoothedMaxRate = smoothedRate;
		} else {
			smoothedMaxRate += (smoothedMinRate - smoothedMaxRate) * weight;
		}
	}

	private void updateSmoothedMin(double weight) {
		double smoothedRate = getSmoothedRate();
		if (smoothedRate < smoothedMinRate) {
			smoothedMinRate = smoothedRate;
		} else {
			smoothedMinRate += (smoothedMaxRate - smoothedMinRate) * weight;
		}
	}

	/**
	 * Returns smoothed value of rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed value of rate in events per second
	 */
	public double getSmoothedRate() {
		if (firstIterationCompleted) {
			return 1.0 / (smoothedPeriod / ONE_SECOND_IN_MILLIS);
		} else {
			return 0.0;
		}
	}

	/**
	 * Returns smoothed minimum rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed minimum rate in events per second.
	 */
	public double getSmoothedMinRate() {
		return firstIterationCompleted ? smoothedMinRate : 0.0;
	}

	/**
	 * Returns smoothed maximum rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed maximum rate in events per second.
	 */
	public double getSmoothedMaxRate() {
		return firstIterationCompleted ? smoothedMaxRate : 0.0;
	}

	/**
	 * Returns total amount of recorded events
	 *
	 * @return total amount of recorded events
	 */
	public long getEventsCount() {
		return totalEvents;
	}

	@Override
	public String toString() {
		return String.format("total: %d   smoothedRate: %.3f   smoothedMinRate: %.4f   smoothedMaxRate: %.4f",
				getEventsCount(), getSmoothedRate(), getSmoothedMinRate(), getSmoothedMaxRate());
	}

	private static double secondsToMillis(double seconds) {
		return seconds * ONE_SECOND_IN_MILLIS;
	}

	private static double transformWindow(double windowBase2InSeconds) {
		return windowBase2InSeconds * ONE_SECOND_IN_MILLIS / log(2);
	}

	public static Accumulator accumulator() {
		return new Accumulator();
	}

	public static final class Accumulator {
		private long eventsCount;
		private double smoothedRate;
		private double smoothedMinRate;
		private double smoothedMaxRate;

		private Accumulator() {
			this.eventsCount = 0L;
			this.smoothedRate = 0.0;
			this.smoothedMinRate = 0.0;
			this.smoothedMaxRate = 0.0;
		}

		public void add(EventsCounter counter) {
			this.eventsCount += counter.getEventsCount();
			this.smoothedRate += counter.getSmoothedRate();
			this.smoothedMaxRate += counter.getSmoothedMaxRate();
			this.smoothedMinRate += counter.getSmoothedMinRate();
		}

		public long getEventsCount() {
			return eventsCount;
		}

		public double getSmoothedRate() {
			return smoothedRate;
		}

		public double getSmoothedMinRate() {
			return smoothedMinRate;
		}

		public double getSmoothedMaxRate() {
			return smoothedMaxRate;
		}

		@Override
		public String toString() {
			return String.format("total: %d   smoothedRate: %.3f   smoothedMinRate: %.3f      smoothedMaxRate: %.3f",
					getEventsCount(), getSmoothedRate(), getSmoothedMinRate(), getSmoothedMaxRate());
		}
	}
}
