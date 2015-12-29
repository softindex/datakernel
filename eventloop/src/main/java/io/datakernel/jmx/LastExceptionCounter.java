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

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;

public final class LastExceptionCounter {
	private final Marker marker;
	private Throwable throwable;
	private Object causeObject;
	private long timestamp;
	private int total;

	public LastExceptionCounter(String name) {
		this(MarkerFactory.getMarker(name));
	}

	public LastExceptionCounter(Marker marker) {
		this.marker = marker;
	}

	public void update(Throwable throwable, Object causeObject, long timestamp) {
		this.total++;
		this.throwable = throwable;
		this.causeObject = causeObject;
		this.timestamp = timestamp;
	}

	public void reset() {
		this.total = 0;
		this.throwable = null;
		this.causeObject = null;
		this.timestamp = 0;
	}

	public Marker getMarker() {
		return marker;
	}

	public int getTotal() {
		return total;
	}

	public String getCauseObject() {
		Object o = causeObject;
		if (o == null)
			return null;
		return o.toString();
	}

	public String getExceptionTimestamp() {
		return MBeanFormat.formatPeriodAgo(timestamp);
	}

	public String[] getFormattedException() {
		return MBeanFormat.formatException(throwable);
	}

	public Throwable getLastException() {
		return throwable;
	}

	public CompositeData compositeData() throws OpenDataException {
		if (total == 0 || throwable == null) {
			return null;
		}
		return CompositeDataBuilder.builder(marker.getName())
				.add("ExceptionMarker", SimpleType.STRING, marker.getName())
				.add("ExceptionType", SimpleType.STRING, throwable.getClass().getSimpleName())
				.add("StackTrace", new ArrayType<>(1, SimpleType.STRING), getFormattedException())
				.add("CauseObject", SimpleType.STRING, getCauseObject())
				.add("Timestamp", SimpleType.STRING, getExceptionTimestamp())
				.add("Total", SimpleType.INTEGER, total)
				.build();
	}

	public static Accumulator accumulator() {
		return new Accumulator();
	}

	public static final class Accumulator {
		private static final String COMPOSITE_DATE_NAME = "Last Exception Accumulator";

		private Throwable throwable;
		private Object causeObject;
		private long timestamp;
		private int total;

		private Accumulator() {
			this.throwable = null;
			this.causeObject = null;
			this.timestamp = 0L;
			this.total = 0;
		}

		public void add(LastExceptionCounter counter) {
			this.total += counter.total;
			if (counter.timestamp > this.timestamp) {
				this.throwable = counter.throwable;
				this.causeObject = counter.causeObject;
				this.timestamp = counter.timestamp;
			}
		}

		public Throwable getLastException() {
			return throwable;
		}

		public Object getCauseObject() {
			return causeObject;
		}

		public long getExceptionTimestamp() {
			return timestamp;
		}

		public int getTotalExceptions() {
			return total;
		}

		public CompositeData compositeData() throws OpenDataException {
			if (total == 0 || throwable == null) {
				return null;
			}
			return CompositeDataBuilder.builder(COMPOSITE_DATE_NAME)
					.add("ExceptionType", SimpleType.STRING, throwable.getClass().getSimpleName())
					.add("StackTrace", new ArrayType<>(1, SimpleType.STRING), MBeanFormat.formatException(throwable))
					.add("CauseObject", SimpleType.STRING, causeObject != null ? causeObject.toString() : "")
					.add("Timestamp", SimpleType.STRING, MBeanFormat.formatPeriodAgo(timestamp))
					.add("Total", SimpleType.INTEGER, total)
					.build();
		}
	}
}
