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

import io.datakernel.jmx.*;

import static io.datakernel.jmx.ValueStats.POWERS_OF_TEN;

public final class RpcRequestStats implements JmxRefreshable {
	private final EventStats totalRequests;
	private final EventStats failedRequests;
	private final EventStats rejectedRequests;
	private final EventStats expiredRequests;
	private final ValueStats responseTime;
	private final ExceptionStats serverExceptions;

	private RpcRequestStats(double smoothingWindow) {
		totalRequests = EventStats.create(smoothingWindow);
		failedRequests = EventStats.create(smoothingWindow);
		rejectedRequests = EventStats.create(smoothingWindow);
		expiredRequests = EventStats.create(smoothingWindow);
		responseTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TEN);
		serverExceptions = ExceptionStats.create();
	}

	public static RpcRequestStats create(double smoothingWindow) {
		return new RpcRequestStats(smoothingWindow);
	}

	public void resetStats() {
		totalRequests.resetStats();
		failedRequests.resetStats();
		rejectedRequests.resetStats();
		expiredRequests.resetStats();
		responseTime.resetStats();
		serverExceptions.resetStats();
	}

	public void add(RpcRequestStats statsSet) {
		totalRequests.add(statsSet.getTotalRequests());
		failedRequests.add(statsSet.getFailedRequests());
		rejectedRequests.add(statsSet.getRejectedRequests());
		expiredRequests.add(statsSet.getExpiredRequests());
		responseTime.add(statsSet.getResponseTime());
		serverExceptions.add(statsSet.getServerExceptions());
	}

	@JmxAttribute
	public EventStats getTotalRequests() {
		return totalRequests;
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

	@JmxAttribute(
			description = "delay between request/response (in milliseconds)",
			extraSubAttributes = "histogram"
	)
	public ValueStats getResponseTime() {
		return responseTime;
	}

	@JmxAttribute
	public ExceptionStats getServerExceptions() {
		return serverExceptions;
	}

	@Override
	public void refresh(long timestamp) {
		totalRequests.refresh(timestamp);
		failedRequests.refresh(timestamp);
		rejectedRequests.refresh(timestamp);
		expiredRequests.refresh(timestamp);
		responseTime.refresh(timestamp);
	}
}
