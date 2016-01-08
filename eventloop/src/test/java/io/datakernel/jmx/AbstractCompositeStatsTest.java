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

import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.openmbean.SimpleType;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class AbstractCompositeStatsTest {

	@Test
	public void itShouldProperlyReturnAttributes() {
		CompositeStatsStub compositeStatsStub = new CompositeStatsStub();
		compositeStatsStub.getCounterOne().recordValue(15);
		compositeStatsStub.getCounterTwo().recordValue(147);

		SortedMap<String, JmxStats.TypeAndValue> attributes = compositeStatsStub.getAttributes();
		assertEquals(4, attributes.size());

		Iterator<String> iterator = attributes.keySet().iterator();
		String attribute_1_name = iterator.next();
		String attribute_2_name = iterator.next();
		String attribute_3_name = iterator.next();
		String attribute_4_name = iterator.next();

		assertEquals("counterOne_count", attribute_1_name);
		assertEquals(SimpleType.INTEGER, attributes.get(attribute_1_name).getType());
		assertEquals(1, attributes.get(attribute_1_name).getValue());

		assertEquals("counterOne_sum", attribute_2_name);
		assertEquals(SimpleType.LONG, attributes.get(attribute_2_name).getType());
		assertEquals(15L, attributes.get(attribute_2_name).getValue());

		assertEquals("counterTwo_count", attribute_3_name);
		assertEquals(SimpleType.INTEGER, attributes.get(attribute_3_name).getType());
		assertEquals(1, attributes.get(attribute_3_name).getValue());

		assertEquals("counterTwo_sum", attribute_4_name);
		assertEquals(SimpleType.LONG, attributes.get(attribute_4_name).getType());
		assertEquals(147L, attributes.get(attribute_4_name).getValue());
	}

	@Test
	public void itShouldProperlyAggregateStats() {
		CompositeStatsStub compositeStatsStub_1 = new CompositeStatsStub();
		compositeStatsStub_1.getCounterOne().recordValue(15);
		compositeStatsStub_1.getCounterTwo().recordValue(147);

		CompositeStatsStub compositeStatsStub_2 = new CompositeStatsStub();
		compositeStatsStub_2.getCounterOne().recordValue(33);
		compositeStatsStub_2.getCounterTwo().recordValue(271);

		CompositeStatsStub accumulator = new CompositeStatsStub();
		accumulator.add(compositeStatsStub_1);
		accumulator.add(compositeStatsStub_2);

		SortedMap<String, JmxStats.TypeAndValue> attributes = accumulator.getAttributes();
		assertEquals(4, attributes.size());

		String attribute_1_name = "counterOne_count";
		String attribute_2_name = "counterOne_sum";
		String attribute_3_name = "counterTwo_count";
		String attribute_4_name = "counterTwo_sum";

		assertEquals(SimpleType.INTEGER, attributes.get(attribute_1_name).getType());
		assertEquals(2, attributes.get(attribute_1_name).getValue());

		assertEquals(SimpleType.LONG, attributes.get(attribute_2_name).getType());
		assertEquals(15L + 33L, attributes.get(attribute_2_name).getValue());

		assertEquals(SimpleType.INTEGER, attributes.get(attribute_3_name).getType());
		assertEquals(2, attributes.get(attribute_3_name).getValue());

		assertEquals(SimpleType.LONG, attributes.get(attribute_4_name).getType());
		assertEquals(147L + 271L, attributes.get(attribute_4_name).getValue());
	}

	@Test
	public void itShouldRefreshAllIncludedStatsProperly() {
		CompositeStatsStub compositeStatsStub = new CompositeStatsStub();
		compositeStatsStub.getCounterOne().recordValue(15);
		compositeStatsStub.getCounterTwo().recordValue(147);

		long timestamp = 18000;
		double window = 5.0;
		compositeStatsStub.refreshStats(timestamp, window);

		double acceptableError = 1E-12;
		assertEquals(1, compositeStatsStub.getCounterOne().getRefreshStatsInvocations());
		assertEquals(timestamp, compositeStatsStub.getCounterOne().getLastTimestamp());
		assertEquals(window, compositeStatsStub.getCounterOne().getLastSmoothingWindow(), acceptableError);

		assertEquals(1, compositeStatsStub.getCounterTwo().getRefreshStatsInvocations());
		assertEquals(timestamp, compositeStatsStub.getCounterTwo().getLastTimestamp());
		assertEquals(window, compositeStatsStub.getCounterTwo().getLastSmoothingWindow(), acceptableError);
	}

	public static class CompositeStatsStub extends AbstractCompositeStats<CompositeStatsStub> {
		private JmxStatsStub counterOne = new JmxStatsStub();
		private JmxStatsStub counterTwo = new JmxStatsStub();

		public JmxStatsStub getCounterOne() {
			return counterOne;
		}

		public JmxStatsStub getCounterTwo() {
			return counterTwo;
		}
	}
}
