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

public final class ExceptionStats implements JmxStats<ExceptionStats> {
	private Throwable throwable;
	private Object causeObject;
	private long timestamp;
	private int count;

	public ExceptionStats() {}

	public void recordException(Throwable throwable, Object causeObject, long timestamp) {
		this.count++;
		this.throwable = throwable;
		this.causeObject = causeObject;
		this.timestamp = timestamp;
	}

	public void resetStats() {
		this.count = 0;
		this.throwable = null;
		this.causeObject = null;
		this.timestamp = 0;
	}

	@Override
	public void add(ExceptionStats counter) {
		this.count += counter.count;
		if (counter.timestamp > this.timestamp) {
			this.throwable = counter.throwable;
			this.causeObject = counter.causeObject;
			this.timestamp = counter.timestamp;
		}
	}

	@Override
	public void refreshStats(long timestamp, double smoothingWindow) {

	}

	@JmxAttribute
	public Throwable getLastException() {
		return throwable;
	}

	@JmxAttribute
	public long getLastExceptionTimestamp() {
		return timestamp;
	}

	@JmxAttribute
	public int getTotal() {
		return count;
	}
}
