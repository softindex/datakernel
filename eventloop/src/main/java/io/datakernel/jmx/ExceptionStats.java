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

public final class ExceptionStats implements JmxStats<ExceptionStats> {
	public static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private Class<? extends Throwable> exceptionClass;
	private int count;
	private long lastExceptionTimestamp;
	private ExceptionSummary summary;
	private ExceptionDetails details;

	private ExceptionStats() {
		summary = new ExceptionSummary();
		assert (details = new ExceptionDetails()) != null;
		assert (summary = null) == null;
	}

	public static ExceptionStats create() {
		return new ExceptionStats();
	}

	public ExceptionStats withStoreStackTrace(boolean store) {
		if (store) {
			details = new ExceptionDetails();
			summary = null;
		} else {
			details = null;
			summary = new ExceptionSummary();
		}
		return this;
	}

	public void recordException(Throwable throwable, Object context) {
		this.count++;
		this.exceptionClass = throwable != null ? throwable.getClass() : null;
		this.lastExceptionTimestamp = System.currentTimeMillis();

		if (details != null) {
			details.throwable = throwable;
			details.context = context;
		}
	}

	public void recordException(Throwable throwable) {
		recordException(throwable, null);
	}

	public void resetStats() {
		this.exceptionClass = null;
		this.count = 0;
		this.lastExceptionTimestamp = 0;
		if (details != null) {
			details.throwable = null;
			details.context = null;
		}
	}

	@Override
	public void add(ExceptionStats another) {
		this.count += another.count;
		if (another.lastExceptionTimestamp >= this.lastExceptionTimestamp) {
			this.exceptionClass = another.exceptionClass;
			this.lastExceptionTimestamp = another.lastExceptionTimestamp;
			if (another.details != null) {
				if (this.details == null) {
					details = new ExceptionDetails();
				}
				this.details.throwable = another.details.throwable;
				this.details.context = another.details.context;
			}
		}
	}

	@JmxAttribute
	public int getTotal() {
		return count;
	}

	@JmxAttribute(optional = true)
	public String getLastType() {
		return exceptionClass != null ? exceptionClass.toString() : null;
	}

	@JmxAttribute(optional = true)
	public String getLastTimestamp() {
		return lastExceptionTimestamp != 0 ? TIMESTAMP_FORMAT.format(new Date(lastExceptionTimestamp)) : null;
	}

	public Throwable getLastException() {
		return details != null ? details.throwable : null;
	}

	@JmxAttribute(optional = true)
	public String getLastMessage() {
		if (details == null) {
			return null;
		}

		if (details.throwable == null) {
			return null;
		}

		return details.throwable.getMessage();
	}

	@JmxAttribute(name = "")
	public ExceptionSummary getSummary() {
		return summary;
	}

	@JmxAttribute(name = "")
	public ExceptionDetails getDetails() {
		return details;
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

	public final class ExceptionDetails {
		private Throwable throwable;
		private Object context;

		@JmxAttribute
		public Object getLastContext() {
			return context;
		}

		@JmxAttribute
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

			StringBuilder summary = new StringBuilder("Total: " + count);

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
	}

	public final class ExceptionSummary {
		@JmxAttribute
		public String getLast() {
			return exceptionClass != null ? exceptionClass.getName() : null;
		}
	}
}
