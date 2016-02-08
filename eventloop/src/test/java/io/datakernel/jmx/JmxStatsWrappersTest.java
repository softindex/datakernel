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

import javax.management.openmbean.SimpleType;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class JmxStatsWrappersTest {

	@Test
	public void itShouldReturnProperAttributesForSummableJmxStats() {
		JmxStats<?> jmxStats = JmxStatsWrappers.forSummableValue(10L);

		SortedMap<String, TypeAndValue> attributes = jmxStats.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.LONG, attributes.get("sum").getType());
		assertEquals(10L, attributes.get("sum").getValue());
	}

	@Test
	public void itShouldAccumulateProperlyForSummableJmxStats() {
		JmxStatsWrappers.JmxSummableValueStats jmxStats_1 = JmxStatsWrappers.forSummableValue(10L);
		JmxStatsWrappers.JmxSummableValueStats jmxStats_2 = JmxStatsWrappers.forSummableValue(21L);

		JmxStatsWrappers.JmxSummableValueStats accumulator = new JmxStatsWrappers.JmxSummableValueStats();
		accumulator.add(jmxStats_1);
		accumulator.add(jmxStats_2);

		SortedMap<String, TypeAndValue> attributes = accumulator.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.LONG, attributes.get("sum").getType());
		assertEquals(10L + 21L, attributes.get("sum").getValue());
	}

	@Test
	public void itShouldReturnProperAttributesForMaxValueJmxStats() {
		JmxStats<?> jmxStats = JmxStatsWrappers.forMaxValue(10L);

		SortedMap<String, TypeAndValue> attributes = jmxStats.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.LONG, attributes.get("max").getType());
		assertEquals(10L, attributes.get("max").getValue());
	}

	@Test
	public void itShouldAccumulateProperlyForMaxValueJmxStats() {
		JmxStatsWrappers.JmxMaxValueStats jmxStats_1 = JmxStatsWrappers.forMaxValue(10L);
		JmxStatsWrappers.JmxMaxValueStats jmxStats_2 = JmxStatsWrappers.forMaxValue(21L);

		JmxStatsWrappers.JmxMaxValueStats accumulator = new JmxStatsWrappers.JmxMaxValueStats();
		accumulator.add(jmxStats_1);
		accumulator.add(jmxStats_2);

		SortedMap<String, TypeAndValue> attributes = accumulator.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.LONG, attributes.get("max").getType());
		assertEquals(21L, attributes.get("max").getValue());
	}

	@Test
	public void itShouldReturnProperAttributesForMinValueJmxStats() {
		JmxStats<?> jmxStats = JmxStatsWrappers.forMinValue(10L);

		SortedMap<String, TypeAndValue> attributes = jmxStats.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.LONG, attributes.get("min").getType());
		assertEquals(10L, attributes.get("min").getValue());
	}

	@Test
	public void itShouldAccumulateProperlyForMinValueJmxStats() {
		JmxStatsWrappers.JmxMinValueStats jmxStats_1 = JmxStatsWrappers.forMinValue(10L);
		JmxStatsWrappers.JmxMinValueStats jmxStats_2 = JmxStatsWrappers.forMinValue(21L);

		JmxStatsWrappers.JmxMinValueStats accumulator = new JmxStatsWrappers.JmxMinValueStats();
		accumulator.add(jmxStats_1);
		accumulator.add(jmxStats_2);

		SortedMap<String, TypeAndValue> attributes = accumulator.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.LONG, attributes.get("min").getType());
		assertEquals(10L, attributes.get("min").getValue());
	}

	@Test
	public void itShouldReturnProperAttributesForAverageValueJmxStats() {
		JmxStats<?> jmxStats = JmxStatsWrappers.forAverageValue(10L);

		SortedMap<String, TypeAndValue> attributes = jmxStats.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.DOUBLE, attributes.get("average").getType());
		double acceptableError = 1E-10;
		assertEquals(10.0, (double) attributes.get("average").getValue(), acceptableError);
	}

	@Test
	public void itShouldAccumulateProperlyForAverageValueJmxStats() {
		JmxStatsWrappers.JmxAverageValueStats jmxStats_1 = JmxStatsWrappers.forAverageValue(10L);
		JmxStatsWrappers.JmxAverageValueStats jmxStats_2 = JmxStatsWrappers.forAverageValue(21L);
		JmxStatsWrappers.JmxAverageValueStats jmxStats_3 = JmxStatsWrappers.forAverageValue(104L);

		JmxStatsWrappers.JmxAverageValueStats accumulator = new JmxStatsWrappers.JmxAverageValueStats();
		accumulator.add(jmxStats_1);
		accumulator.add(jmxStats_2);
		accumulator.add(jmxStats_3);

		SortedMap<String, TypeAndValue> attributes = accumulator.getAttributes();
		assertEquals(1, attributes.size());
		assertEquals(SimpleType.DOUBLE, attributes.get("average").getType());
		assertEquals((10L + 21L + 104L) / 3.0, attributes.get("average").getValue());
	}
}
