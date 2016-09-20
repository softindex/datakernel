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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;

public final class ExceptionStats implements JmxStats<ExceptionStats> {
	public static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private Throwable throwable;
	private Object causeObject;
	private int count;
	private long lastExceptionTimestamp;

	private ExceptionStats() {}

	public static ExceptionStats create() {return new ExceptionStats();}

	public void recordException(Throwable throwable, Object causeObject) {
		this.count++;
		this.throwable = throwable;
		this.causeObject = causeObject;
		this.lastExceptionTimestamp = System.currentTimeMillis();
	}

	public void resetStats() {
		this.count = 0;
		this.throwable = null;
		this.causeObject = null;
	}

	@Override
	public void add(ExceptionStats another) {
		this.count += another.count;
		if (another.lastExceptionTimestamp > this.lastExceptionTimestamp) {
			this.throwable = another.throwable;
			this.causeObject = another.causeObject;
			this.lastExceptionTimestamp = another.lastExceptionTimestamp;
		}
	}

	@JmxAttribute
	public int getTotal() {
		return count;
	}

	@JmxAttribute
	public String getLastExceptionType() {
		return throwable != null ? throwable.getClass().getName() : "";
	}

	@JmxAttribute
	public String getLastExceptionTimestamp() {
		return TIMESTAMP_FORMAT.format(new Date(lastExceptionTimestamp));
	}

	public Throwable getLastException() {
		return throwable;
	}

	@JmxAttribute
	public String getLastExceptionMessage() {
		return throwable != null ? throwable.getMessage() : "";
	}

	@JmxAttribute
	public Object getLastExceptionCause() {
		return causeObject;
	}

	@JmxAttribute
	public List<String> getLastExceptionStackTrace() {
		if (throwable != null) {
			return asList(MBeanFormat.formatException(throwable));
		} else {
			return new ArrayList<>();
		}
	}
}
