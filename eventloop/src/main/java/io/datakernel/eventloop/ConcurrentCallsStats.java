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

package io.datakernel.eventloop;

import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;

import java.util.HashMap;
import java.util.Map;

public final class ConcurrentCallsStats {
	private final EventStats totalCalls;
	private final EventStats rejectedCalls;
	private final ValueStats executionDuration;
	private final ValueStats awaitingStartDuration;

	private double smoothingWindow;

	private final Map<String, DistributedStats> taskNameToStats = new HashMap<>();

	// region builders
	private ConcurrentCallsStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;

		totalCalls = EventStats.create().withSmoothingWindow(smoothingWindow);
		rejectedCalls = EventStats.create().withSmoothingWindow(smoothingWindow);
		executionDuration = ValueStats.create().withSmoothingWindow(smoothingWindow);
		awaitingStartDuration = ValueStats.create().withSmoothingWindow(smoothingWindow);
	}

	public static ConcurrentCallsStats create(double smoothingWindow) {
		return new ConcurrentCallsStats(smoothingWindow);
	}
	// region builders

	public void recordCall(String taskName) {
		totalCalls.recordEvent();
		ensureConcurrentCallsStatsForTaskName(taskName).getTotalCalls().recordEvent();
	}

	public void recordRejectedCall(String taskName) {
		rejectedCalls.recordEvent();
		ensureConcurrentCallsStatsForTaskName(taskName).getRejectedCalls().recordEvent();
	}

	public void recordCallDuration(String taskName, int duration) {
		executionDuration.recordValue(duration);
		ensureConcurrentCallsStatsForTaskName(taskName).getExecutionDuration().recordValue(duration);
	}

	public void recordAwaitingStartDuration(String taskName, int duration) {
		awaitingStartDuration.recordValue(duration);
		ensureConcurrentCallsStatsForTaskName(taskName).getAwaitingStartDuration().recordValue(duration);
	}

	@JmxAttribute
	public EventStats getTotalCalls() {
		return totalCalls;
	}

	@JmxAttribute
	public EventStats getRejectedCalls() {
		return rejectedCalls;
	}

	@JmxAttribute(description = "execution duration of one task (in milliseconds)")
	public ValueStats getExecutionDuration() {
		return executionDuration;
	}

	@JmxAttribute(description = "awaiting start duration of one task (in milliseconds)")
	public ValueStats getAwaitingStartDuration() {
		return awaitingStartDuration;
	}

	@JmxAttribute
	public Map<String, DistributedStats> getTaskNameToStats() {
		return taskNameToStats;
	}

	public static final class DistributedStats {
		private final EventStats totalCalls;
		private final EventStats rejectedCalls;
		private final ValueStats executionDuration;
		private final ValueStats awaitingStartDuration;

		public DistributedStats(double smoothingWindow) {
			totalCalls = EventStats.create().withSmoothingWindow(smoothingWindow);
			rejectedCalls = EventStats.create().withSmoothingWindow(smoothingWindow);
			executionDuration = ValueStats.create().withSmoothingWindow(smoothingWindow);
			awaitingStartDuration = ValueStats.create().withSmoothingWindow(smoothingWindow);
		}

		@JmxAttribute
		public EventStats getTotalCalls() {
			return totalCalls;
		}

		@JmxAttribute
		public EventStats getRejectedCalls() {
			return rejectedCalls;
		}

		@JmxAttribute
		public ValueStats getExecutionDuration() {
			return executionDuration;
		}

		@JmxAttribute
		public ValueStats getAwaitingStartDuration() {
			return awaitingStartDuration;
		}

		private void setSmoothingWindow(double smoothingWindow) {
			totalCalls.setSmoothingWindow(smoothingWindow);
			rejectedCalls.setSmoothingWindow(smoothingWindow);
			executionDuration.setSmoothingWindow(smoothingWindow);
			awaitingStartDuration.setSmoothingWindow(smoothingWindow);
		}
	}

	private DistributedStats ensureConcurrentCallsStatsForTaskName(String taskName) {
		if (!taskNameToStats.containsKey(taskName)) {
			DistributedStats stats = new DistributedStats(smoothingWindow);
			taskNameToStats.put(taskName, stats);
		}
		return taskNameToStats.get(taskName);
	}

	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;

		totalCalls.setSmoothingWindow(smoothingWindow);
		rejectedCalls.setSmoothingWindow(smoothingWindow);
		executionDuration.setSmoothingWindow(smoothingWindow);
		awaitingStartDuration.setSmoothingWindow(smoothingWindow);

		for (DistributedStats distributedStats : taskNameToStats.values()) {
			distributedStats.setSmoothingWindow(smoothingWindow);
		}
	}
}
