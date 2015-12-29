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
import io.datakernel.time.CurrentTimeProvider;

public final class RpcJmxConnectsStatsSet {
	private double smoothingWindow;
	private double smoothingPrecision;

	private final EventsCounter successfulConnects;
	private final EventsCounter failedConnects;
	private final EventsCounter closedConnects;

	public RpcJmxConnectsStatsSet(double smoothingWindow, double smoothingPrecision, CurrentTimeProvider timeProvider) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;

		this.successfulConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.failedConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.closedConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
	}

	public void reset() {
		resetStatsSet(smoothingWindow, smoothingPrecision);
	}

	public void reset(double smoothingWindow, double smoothingPrecision) {
		resetStatsSet(smoothingWindow, smoothingPrecision);
	}

	private void resetStatsSet(double smoothingWindow, double smoothingPrecision) {
		successfulConnects.reset(smoothingWindow, smoothingPrecision);
		failedConnects.reset(smoothingWindow, smoothingPrecision);
		closedConnects.reset(smoothingWindow, smoothingPrecision);
	}

	public EventsCounter getSuccessfulConnects() {
		return successfulConnects;
	}

	public EventsCounter getFailedConnects() {
		return failedConnects;
	}

	public EventsCounter getClosedConnects() {
		return closedConnects;
	}

	public static Accumulator accumulator() {
		return new Accumulator();
	}

	public static final class Accumulator {
		private final EventsCounter.Accumulator successfulConnects;
		private final EventsCounter.Accumulator failedConnects;
		private final EventsCounter.Accumulator closedConnects;

		private Accumulator() {
			this.successfulConnects = EventsCounter.accumulator();
			this.failedConnects = EventsCounter.accumulator();
			this.closedConnects = EventsCounter.accumulator();
		}

		public void add(RpcJmxConnectsStatsSet statsSet) {
			successfulConnects.add(statsSet.getSuccessfulConnects());
			failedConnects.add(statsSet.getFailedConnects());
			closedConnects.add(statsSet.getClosedConnects());
		}

		public EventsCounter.Accumulator getSuccessfulConnects() {
			return successfulConnects;
		}

		public EventsCounter.Accumulator getFailedConnects() {
			return failedConnects;
		}

		public EventsCounter.Accumulator getClosedConnects() {
			return closedConnects;
		}
	}
}
