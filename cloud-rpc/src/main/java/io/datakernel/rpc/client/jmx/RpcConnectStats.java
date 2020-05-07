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

import io.datakernel.jmx.api.attribute.JmxAttribute;
import io.datakernel.jmx.api.attribute.JmxReducers.JmxReducerSum;

public final class RpcConnectStats {
	public long successfulConnects;
	public long failedConnects;
	public long closedConnects;

	public void reset() {
		successfulConnects = failedConnects = closedConnects = 0;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getSuccessfulConnects() {
		return successfulConnects;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getFailedConnects() {
		return failedConnects;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getClosedConnects() {
		return closedConnects;
	}

}
