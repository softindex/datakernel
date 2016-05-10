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
	private final EventStats successfulConnects = new EventStats();
	private final EventStats failedConnects = new EventStats();
	private final EventStats closedConnects = new EventStats();

	public void reset() {
		successfulConnects.resetStats();
		failedConnects.resetStats();
		closedConnects.resetStats();
	}

	public void add(RpcConnectStats statsSet) {
		successfulConnects.add(statsSet.getSuccessfulConnects());
		failedConnects.add(statsSet.getFailedConnects());
		closedConnects.add(statsSet.getClosedConnects());
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

}
