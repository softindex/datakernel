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

public final class RpcRequestStats implements JmxRefreshable {
	private final EventStats totalRequests = EventStats.create();
	private final EventStats successfulRequests = EventStats.create();
	private final EventStats failedRequests = EventStats.create();
	private final EventStats rejectedRequests = EventStats.create();
	private final EventStats expiredRequests = EventStats.create();
	private final ValueStats responseTimeStats = ValueStats.create();
	private final ExceptionStats serverExceptions = ExceptionStats.create();

	private RpcRequestStats() {}

	public static RpcRequestStats create() {return new RpcRequestStats();}

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

	@JmxAttribute(description = "delay between request/response (in milliseconds)")
	public ValueStats getResponseTime() {
		return responseTimeStats;
	}

	@JmxAttribute
	public ExceptionStats getServerExceptions() {
		return serverExceptions;
	}

	@Override
	public void refresh(long timestamp) {
		totalRequests.refresh(timestamp);
		successfulRequests.refresh(timestamp);
		failedRequests.refresh(timestamp);
		rejectedRequests.refresh(timestamp);
		expiredRequests.refresh(timestamp);
		responseTimeStats.refresh(timestamp);
	}
}
