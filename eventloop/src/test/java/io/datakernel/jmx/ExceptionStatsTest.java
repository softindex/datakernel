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

import org.junit.Before;
import org.junit.Test;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import static org.junit.Assert.assertEquals;

public class ExceptionStatsTest {
	String[] exceptionDetailsItemNames;
	OpenType<?>[] exceptionDetailsItemTypes;

	@Before
	public void before() throws OpenDataException {
		exceptionDetailsItemNames = new String[]{
				"lastException",
				"lastExceptionCausedObject",
				"lastExceptionStackTrace",
				"lastExceptionTimestamp",
				"totalExceptions"
		};

		exceptionDetailsItemTypes = new OpenType<?>[]{
				SimpleType.STRING,
				SimpleType.STRING,
				new ArrayType<>(1, SimpleType.STRING),
				SimpleType.LONG,
				SimpleType.INTEGER
		};
	}

	@Test
	public void itShouldProperlyCollectAttributes() throws OpenDataException {
		ExceptionStats stats = new ExceptionStats();
		Exception exception = new RuntimeException("msg");
		Object causedObject = "cause";
		long timestamp = 1000L;
		stats.recordException(exception, causedObject, timestamp);

		assertEquals("msg", stats.getLastExceptionMessage());
		assertEquals(timestamp, stats.getLastExceptionTimestamp());
		assertEquals(1, stats.getTotal());
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

//		// check
		// exception in stats_2 has most recent timestamp
		assertEquals("msg-2", accumulator.getLastExceptionMessage());
		assertEquals(timestamp_2, accumulator.getLastExceptionTimestamp());

		assertEquals(3, accumulator.getTotal());
	}
}
