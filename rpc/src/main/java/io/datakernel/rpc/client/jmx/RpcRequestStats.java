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
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;

public final class RpcRequestStats {
	private final EventStats totalRequests = new EventStats();
	private final EventStats successfulRequests = new EventStats();
	private final EventStats failedRequests = new EventStats();
	private final EventStats rejectedRequests = new EventStats();
	private final EventStats expiredRequests = new EventStats();
	private final ValueStats responseTimeStats = new ValueStats();
	private final ExceptionStats serverExceptions = new ExceptionStats();

	public void resetStats() {
		totalRequests.resetStats();
		successfulRequests.resetStats();
		failedRequests.resetStats();
		rejectedRequests.resetStats();
		expiredRequests.resetStats();
		responseTimeStats.resetStats();
		serverExceptions.resetStats();
	}

	public void add(RpcRequestStats statsSet) {
		totalRequests.add(statsSet.getTotalRequests());
		successfulRequests.add(statsSet.getSuccessfulRequests());
		failedRequests.add(statsSet.getFailedRequests());
		rejectedRequests.add(statsSet.getRejectedRequests());
		expiredRequests.add(statsSet.getExpiredRequests());
		responseTimeStats.add(statsSet.getResponseTime());
		serverExceptions.add(statsSet.getServerExceptions());
	}

	@JmxAttribute
	public EventStats getTotalRequests() {
		return totalRequests;
	}

	@JmxAttribute
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute
	public EventStats getRejectedRequests() {
		return rejectedRequests;
	}

	@JmxAttribute
	public EventStats getExpiredRequests() {
		return expiredRequests;
	}

	@JmxAttribute
	public ValueStats getResponseTime() {
		return responseTimeStats;
	}

	@JmxAttribute
	public ExceptionStats getServerExceptions() {
		return serverExceptions;
	}

}
