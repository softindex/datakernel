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

import org.junit.Test;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class ExceptionStatsTest {

	@Test
	public void itShouldProperlyCollectAttributes() throws OpenDataException {
		ExceptionStats stats = new ExceptionStats();
		Exception exception = new RuntimeException("msg");
		Object causedObject = "cause";
		long timestamp = 1000L;
		stats.recordException(exception, causedObject, timestamp);

		SortedMap<String, JmxStats.TypeAndValue> attributes = stats.getAttributes();
		assertEquals(5, attributes.size());

		String exceptionTypeKey = "lastException";
		assertEquals(SimpleType.STRING, attributes.get(exceptionTypeKey).getType());
		assertEquals(exception.toString(), attributes.get(exceptionTypeKey).getValue());

		String causedObjectKey = "lastExceptionCausedObject";
		assertEquals(SimpleType.STRING, attributes.get(causedObjectKey).getType());
		assertEquals(causedObject, attributes.get(causedObjectKey).getValue());

		String exceptionStackTraceKey = "lastExceptionStackTrace";
		assertEquals(new ArrayType<>(1, SimpleType.STRING), attributes.get(exceptionStackTraceKey).getType());

		String timestampKey = "lastExceptionTimestamp";
		assertEquals(SimpleType.LONG, attributes.get(timestampKey).getType());
		assertEquals(timestamp, attributes.get(timestampKey).getValue());

		String exceptionCountKey = "totalExceptions";
		assertEquals(SimpleType.INTEGER, attributes.get(exceptionCountKey).getType());
		assertEquals(1, attributes.get(exceptionCountKey).getValue());
	}

	@Test
	public void itShouldProperlyAggregateAttributes() throws OpenDataException {
		// init and record
		ExceptionStats stats_1 = new ExceptionStats();
		Exception exception_1 = new RuntimeException("msg-1");
		Object causedObject_1 = "cause-1";
		long timestamp_1 = 1000L;
		stats_1.recordException(exception_1, causedObject_1, timestamp_1);

		ExceptionStats stats_2 = new ExceptionStats();
		Exception exception_2 = new RuntimeException("msg-2");
		Object causedObject_2 = "cause-2";
		long timestamp_2 = 2000L;
		stats_2.recordException(exception_2, causedObject_2, timestamp_2);

		ExceptionStats stats_3 = new ExceptionStats();
		Exception exception_3 = new RuntimeException("msg-3");
		Object causedObject_3 = "cause-3";
		long timestamp_3 = 1500L;
		stats_3.recordException(exception_3, causedObject_3, timestamp_3);

		// aggregate
		ExceptionStats accumulator = new ExceptionStats();
		accumulator.add(stats_1);
		accumulator.add(stats_2);
		accumulator.add(stats_3);

		// check
		SortedMap<String, JmxStats.TypeAndValue> attributes = accumulator.getAttributes();
		assertEquals(5, attributes.size());

		// exception in stats_2 has most recent timestamp
		String exceptionTypeKey = "lastException";
		assertEquals(SimpleType.STRING, attributes.get(exceptionTypeKey).getType());
		assertEquals(exception_2.toString(), attributes.get(exceptionTypeKey).getValue());

		String causedObjectKey = "lastExceptionCausedObject";
		assertEquals(SimpleType.STRING, attributes.get(causedObjectKey).getType());
		assertEquals(causedObject_2, attributes.get(causedObjectKey).getValue());

		String exceptionStackTraceKey = "lastExceptionStackTrace";
		assertEquals(new ArrayType<>(1, SimpleType.STRING), attributes.get(exceptionStackTraceKey).getType());

		String timestampKey = "lastExceptionTimestamp";
		assertEquals(SimpleType.LONG, attributes.get(timestampKey).getType());
		assertEquals(timestamp_2, attributes.get(timestampKey).getValue());

		String exceptionCountKey = "totalExceptions";
		assertEquals(SimpleType.INTEGER, attributes.get(exceptionCountKey).getType());
		assertEquals(3, attributes.get(exceptionCountKey).getValue());
	}
}
