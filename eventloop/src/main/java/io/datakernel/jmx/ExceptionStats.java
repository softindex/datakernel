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

public final class ExceptionStats {
	private final Marker marker;
	private Throwable throwable;
	private Object causeObject;
	private long timestamp;
	private int count;

	public ExceptionStats(String name) {
		this(MarkerFactory.getMarker(name));
	}

	public ExceptionStats(Marker marker) {
		this.marker = marker;
	}

	public void update(Throwable throwable, Object causeObject, long timestamp) {
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

	public void add(ExceptionStats counter) {
		this.count += counter.count;
		if (counter.timestamp > this.timestamp) {
			this.throwable = counter.throwable;
			this.causeObject = counter.causeObject;
			this.timestamp = counter.timestamp;
		}
	}

	public Marker getMarker() {
		return marker;
	}

	public int getCount() {
		return count;
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

	public String getLastException() {
		return throwable != null ? throwable.toString() : "";
	}

	public CompositeData compositeData() throws OpenDataException {
		if (count == 0 || throwable == null) {
			return null;
		}
		return CompositeDataBuilder.builder(marker.getName())
				.add("ExceptionMarker", SimpleType.STRING, marker.getName())
				.add("ExceptionType", SimpleType.STRING, throwable.getClass().getSimpleName())
				.add("StackTrace", new ArrayType<>(1, SimpleType.STRING), getFormattedException())
				.add("CauseObject", SimpleType.STRING, getCauseObject())
				.add("Timestamp", SimpleType.STRING, getExceptionTimestamp())
				.add("Total", SimpleType.INTEGER, count)
				.build();
	}

}
