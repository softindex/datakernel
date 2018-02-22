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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;

public final class ExceptionStats implements JmxStats<ExceptionStats>, JmxStatsWithReset {
	public static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static final long DETAILS_REFRESH_TIMEOUT = 1000L;

	private Class<? extends Throwable> exceptionClass;
	private int count;
	private long lastExceptionTimestamp;
	private Throwable throwable;
	private Object context;

	private ExceptionStats() {
	}

	public static ExceptionStats create() {
		return new ExceptionStats();
	}

	public void recordException(Throwable throwable, Object context) {
		this.count++;
		long now = System.currentTimeMillis();

		if (now >= lastExceptionTimestamp + DETAILS_REFRESH_TIMEOUT) {
			this.exceptionClass = throwable != null ? throwable.getClass() : null;
			this.throwable = throwable;
			this.context = context;
			this.lastExceptionTimestamp = now;
		}
	}

	public void recordException(Throwable throwable) {
		recordException(throwable, null);
	}

	@Override
	public void resetStats() {
		this.count = 0;
		this.lastExceptionTimestamp = 0;

		this.exceptionClass = null;
		this.throwable = null;
		this.context = null;
	}

	@Override
	public void add(ExceptionStats another) {
		this.count += another.count;
		if (another.lastExceptionTimestamp >= this.lastExceptionTimestamp) {
			this.lastExceptionTimestamp = another.lastExceptionTimestamp;

			this.exceptionClass = another.exceptionClass;
			this.throwable = another.throwable;
			this.context = another.context;
		}
	}

	@JmxAttribute(optional = true)
	public int getTotal() {
		return count;
	}

	@JmxAttribute(optional = true)
	public String getLastType() {
		return exceptionClass != null ? exceptionClass.toString() : null;
	}

	@JmxAttribute(optional = true)
	public long getLastTimestamp() {
		return lastExceptionTimestamp;
	}

	@JmxAttribute(optional = true)
	public String getLastTime() {
		return lastExceptionTimestamp != 0 ? TIMESTAMP_FORMAT.format(new Date(lastExceptionTimestamp)) : null;
	}

	public Throwable getLastException() {
		return throwable;
	}

	@JmxAttribute(optional = true)
	public String getLastMessage() {
		return throwable != null ? throwable.getMessage() : null;
	}

	@JmxAttribute(optional = true)
	public Object getLastContext() {
		return context;
	}

	@JmxAttribute(optional = true)
	public List<String> getLastStackTrace() {
		if (throwable != null) {
			return asList(MBeanFormat.formatException(throwable));
		} else {
			return null;
		}
	}

	@JmxAttribute
	public String getSummary() {
		if (count == 0) return "";

		StringBuilder summary = new StringBuilder("Count: " + count +
				" " + MBeanFormat.formatTimestamp(lastExceptionTimestamp));

		if (throwable != null) {
			summary.append("\n\nStack Trace: ");
			summary.append(MBeanFormat.formatExceptionLine(throwable).trim());
		}

		if (context != null) {
			summary.append("\n\nContext: ");
			summary.append(context.toString());
		}

		return summary.toString();
	}

	@Override
	public String toString() {
		String last = "";
		if (exceptionClass != null) {
			last = "; " + exceptionClass.getSimpleName();
			last += " @ " + getLastTimestamp();
		}
		return Integer.toString(count) + last;
	}

}
