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

package io.datakernel.jmx.stats;

import javax.management.openmbean.*;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static io.datakernel.jmx.utils.MBeanFormat.formatException;
import static io.datakernel.jmx.utils.Utils.stringOf;

public final class ExceptionStats implements JmxStats<ExceptionStats> {
	private static final String DETAILS_TYPE_NAME = "ExceptionStatsDetails";
	private static final String DETAILS_KEY = "details";
	private static final String LAST_EXCEPTION_KEY = "lastException";
	private static final String CAUSED_OBJECT_KEY = "lastExceptionCausedObject";
	private static final String STACK_TRACE_KEY = "lastExceptionStackTrace";
	private static final String TIMESTAMP_KEY = "lastExceptionTimestamp";
	private static final String TOTAL_EXCEPTIONS_KEY = "totalExceptions";

	private final CompositeType detailsType;

	private Throwable throwable;
	private Object causeObject;
	private long timestamp;
	private int count;

	public ExceptionStats() {
		try {
			String[] detailsItemNames = new String[]{
					LAST_EXCEPTION_KEY,
					CAUSED_OBJECT_KEY,
					STACK_TRACE_KEY,
					TIMESTAMP_KEY,
					TOTAL_EXCEPTIONS_KEY
			};

			OpenType<?>[] detailsItemTypes = new OpenType<?>[]{
					SimpleType.STRING,
					SimpleType.STRING,
					new ArrayType<>(1, SimpleType.STRING),
					SimpleType.LONG,
					SimpleType.INTEGER
			};

			detailsType = new CompositeType(
					DETAILS_TYPE_NAME, DETAILS_TYPE_NAME, detailsItemNames, detailsItemNames, detailsItemTypes);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

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

	@Override
	public SortedMap<String, TypeAndValue> getAttributes() {
		SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
		attributes.put(LAST_EXCEPTION_KEY, new TypeAndValue(SimpleType.STRING, stringOf(throwable)));
		attributes.put(TOTAL_EXCEPTIONS_KEY, new TypeAndValue(SimpleType.INTEGER, count));
		try {
			Map<String, Object> details = new HashMap<>();
			details.put(LAST_EXCEPTION_KEY, stringOf(throwable));
			details.put(CAUSED_OBJECT_KEY, stringOf(causeObject));
			details.put(STACK_TRACE_KEY, formatException(throwable));
			details.put(TIMESTAMP_KEY, timestamp);
			details.put(TOTAL_EXCEPTIONS_KEY, count);
			CompositeDataSupport compositeDataSupport = new CompositeDataSupport(detailsType, details);
			attributes.put(DETAILS_KEY, new TypeAndValue(detailsType, compositeDataSupport));
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
		return attributes;
	}

	public Throwable getThrowable() {
		return throwable;
	}

	public Object getCauseObject() {
		return causeObject;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getCount() {
		return count;
	}
}
