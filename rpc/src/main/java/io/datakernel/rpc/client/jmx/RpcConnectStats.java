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

import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;

public final class RpcConnectStats {
	private final EventStats successfulConnects;
	private final EventStats failedConnects;
	private final EventStats closedConnects;

	public RpcConnectStats(double smoothingWindow) {
		successfulConnects = new EventStats(smoothingWindow);
		failedConnects = new EventStats(smoothingWindow);
		closedConnects = new EventStats(smoothingWindow);
	}

	@JmxAttribute
	public EventStats getSuccessfulConnects() {
		return successfulConnects;
	}

	@JmxAttribute
	public EventStats getFailedConnects() {
		return failedConnects;
	}

	@JmxAttribute
	public EventStats getClosedConnects() {
		return closedConnects;
	}

	public void resetStats() {
		successfulConnects.resetStats();
		failedConnects.resetStats();
		closedConnects.resetStats();
	}

	public void setSmoothingWindow(double smoothingWindow) {
		successfulConnects.setSmoothingWindow(smoothingWindow);
		failedConnects.setSmoothingWindow(smoothingWindow);
		closedConnects.setSmoothingWindow(smoothingWindow);
	}
}
