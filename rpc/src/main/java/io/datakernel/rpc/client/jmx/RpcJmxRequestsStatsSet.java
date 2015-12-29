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

package io.datakernel.rpc.client.jmx;

import io.datakernel.jmx.EventsCounter;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.jmx.ValuesCounter;
import io.datakernel.time.CurrentTimeProvider;

public final class RpcJmxRequestsStatsSet {
	private static final String LAST_SERVER_EXCEPTION_COUNTER_NAME = "Server exception";

	private double smoothingWindow;
	private double smoothingPrecision;

	private final EventsCounter totalRequests;
	private final EventsCounter successfulRequests;
	private final EventsCounter failedRequests;
	private final EventsCounter rejectedRequests;
	private final EventsCounter expiredRequests;
	private final ValuesCounter responseTimeStats;
	private final LastExceptionCounter lastServerException;

	public RpcJmxRequestsStatsSet(double smoothingWindow, double smoothingPrecision, CurrentTimeProvider timeProvider) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;

		this.totalRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.successfulRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.failedRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.rejectedRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.expiredRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.responseTimeStats = new ValuesCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.lastServerException = new LastExceptionCounter(LAST_SERVER_EXCEPTION_COUNTER_NAME);
	}

	public void reset() {
		resetStatsSet(smoothingWindow, smoothingPrecision);
	}

	public void reset(double smoothingWindow, double smoothingPrecision) {
		resetStatsSet(smoothingWindow, smoothingPrecision);
	}

	private void resetStatsSet(double smoothingWindow, double smoothingPrecision) {
		totalRequests.reset(smoothingWindow, smoothingPrecision);
		successfulRequests.reset(smoothingWindow, smoothingPrecision);
		failedRequests.reset(smoothingWindow, smoothingPrecision);
		rejectedRequests.reset(smoothingWindow, smoothingPrecision);
		expiredRequests.reset(smoothingWindow, smoothingPrecision);
		responseTimeStats.reset(smoothingWindow, smoothingPrecision);
		lastServerException.reset();
	}

	public EventsCounter getTotalRequests() {
		return totalRequests;
	}

	public EventsCounter getSuccessfulRequests() {
		return successfulRequests;
	}

	public EventsCounter getFailedRequests() {
		return failedRequests;
	}

	public EventsCounter getRejectedRequests() {
		return rejectedRequests;
	}

	public EventsCounter getExpiredRequests() {
		return expiredRequests;
	}

	public ValuesCounter getResponseTimeStats() {
		return responseTimeStats;
	}

	public LastExceptionCounter getLastServerExceptionCounter() {
		return lastServerException;
	}

	public static Accumulator accumulator() {
		return new Accumulator();
	}

	public static final class Accumulator {
		private final EventsCounter.Accumulator totalRequests;
		private final EventsCounter.Accumulator successfulRequests;
		private final EventsCounter.Accumulator failedRequests;
		private final EventsCounter.Accumulator rejectedRequests;
		private final EventsCounter.Accumulator expiredRequests;
		private final ValuesCounter.Accumulator responseTimeStats;
		private final LastExceptionCounter.Accumulator lastServerException;

		private Accumulator() {
			this.totalRequests = EventsCounter.accumulator();
			this.successfulRequests = EventsCounter.accumulator();
			this.failedRequests = EventsCounter.accumulator();
			this.rejectedRequests = EventsCounter.accumulator();
			this.expiredRequests = EventsCounter.accumulator();
			this.responseTimeStats = ValuesCounter.accumulator();
			this.lastServerException = LastExceptionCounter.accumulator();
		}

		public void add(RpcJmxRequestsStatsSet statsSet) {
			totalRequests.add(statsSet.getTotalRequests());
			successfulRequests.add(statsSet.getSuccessfulRequests());
			failedRequests.add(statsSet.getFailedRequests());
			rejectedRequests.add(statsSet.getRejectedRequests());
			expiredRequests.add(statsSet.getExpiredRequests());
			responseTimeStats.add(statsSet.getResponseTimeStats());
			lastServerException.add(statsSet.getLastServerExceptionCounter());
		}

		public EventsCounter.Accumulator getTotalRequests() {
			return totalRequests;
		}

		public EventsCounter.Accumulator getSuccessfulRequests() {
			return successfulRequests;
		}

		public EventsCounter.Accumulator getFailedRequests() {
			return failedRequests;
		}

		public EventsCounter.Accumulator getRejectedRequests() {
			return rejectedRequests;
		}

		public EventsCounter.Accumulator getExpiredRequests() {
			return expiredRequests;
		}

		public ValuesCounter.Accumulator getResponseTimeStats() {
			return responseTimeStats;
		}

		public LastExceptionCounter.Accumulator getLastServerException() {
			return lastServerException;
		}
	}
}
