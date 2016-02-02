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

import io.datakernel.jmx.helper.CompositeStatsStub;
import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JmxMBeansStatsTest {
	private static final String GROUPED_STATS_COUNTER_ONE_COUNT = "groupedStats_counterOne_count";
	private static final String GROUPED_STATS_COUNTER_ONE_SUM = "groupedStats_counterOne_sum";
	private static final String GROUPED_STATS_COUNTER_TWO_COUNT = "groupedStats_counterTwo_count";
	private static final String GROUPED_STATS_COUNTER_TWO_SUM = "groupedStats_counterTwo_sum";
	private static final String SIMPLE_STATS_COUNT = "simpleStats_count";
	private static final String SIMPLE_STATS_SUM = "simpleStats_sum";

	// sorted alphabetically
	private static final List<String> ALL_ATTRIBUTE_NAMES_LIST = asList(
			GROUPED_STATS_COUNTER_ONE_COUNT,
			GROUPED_STATS_COUNTER_ONE_SUM,
			GROUPED_STATS_COUNTER_TWO_COUNT,
			GROUPED_STATS_COUNTER_TWO_SUM,
			SIMPLE_STATS_COUNT,
			SIMPLE_STATS_SUM
	);

	private static final String[] ALL_ATTRIBUTE_NAMES_ARRAY =
			ALL_ATTRIBUTE_NAMES_LIST.toArray(new String[ALL_ATTRIBUTE_NAMES_LIST.size()]);

	@Test
	public void itShouldCollectAllJmxStatsFromMonitorableAndWriteThemToMBeanInfo() throws Exception {
		MonitorableStub monitorable = new MonitorableStub();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mBeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}
		Collections.sort(attributeNames);

		List<String> expectedAttributeNames = ALL_ATTRIBUTE_NAMES_LIST;
		assertEquals(expectedAttributeNames, attributeNames);
	}

	@Test
	public void itShouldFetchSingleAttributeValueCorrectly() throws Exception {
		MonitorableStub monitorable = new MonitorableStub();
		monitorable.getGroupedStats().getCounterOne().recordValue(23L);
		monitorable.getGroupedStats().getCounterTwo().recordValue(35L);
		monitorable.getSimpleStats().recordValue(51L);
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		// check init values of attributes
		assertEquals(1, (int) mbean.getAttribute(GROUPED_STATS_COUNTER_ONE_COUNT));
		assertEquals(23L, (long) mbean.getAttribute(GROUPED_STATS_COUNTER_ONE_SUM));
		assertEquals(1, (int) mbean.getAttribute(GROUPED_STATS_COUNTER_TWO_COUNT));
		assertEquals(35L, (long) mbean.getAttribute(GROUPED_STATS_COUNTER_TWO_SUM));
		assertEquals(1, (int) mbean.getAttribute(SIMPLE_STATS_COUNT));
		assertEquals(51L, (long) mbean.getAttribute(SIMPLE_STATS_SUM));

		// modify monitorable object
		monitorable.getGroupedStats().getCounterOne().recordValue(102L);
		// we don't write anything to groupedStats_counterTwo
		monitorable.getSimpleStats().recordValue(207L);

		// check modified values of attributes
		assertEquals(2, (int) mbean.getAttribute(GROUPED_STATS_COUNTER_ONE_COUNT));
		assertEquals(23L + 102L, (long) mbean.getAttribute(GROUPED_STATS_COUNTER_ONE_SUM));
		assertEquals(1, (int) mbean.getAttribute(GROUPED_STATS_COUNTER_TWO_COUNT));
		assertEquals(35L, (long) mbean.getAttribute(GROUPED_STATS_COUNTER_TWO_SUM));
		assertEquals(2, (int) mbean.getAttribute(SIMPLE_STATS_COUNT));
		assertEquals(51L + 207L, (long) mbean.getAttribute(SIMPLE_STATS_SUM));
	}

	@Test
	public void itShouldFetchBunchOfAttributeValuesCorrectly() throws Exception {
		MonitorableStub monitorable = new MonitorableStub();
		monitorable.getGroupedStats().getCounterOne().recordValue(23L);
		monitorable.getGroupedStats().getCounterTwo().recordValue(35L);
		monitorable.getSimpleStats().recordValue(51L);
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		// check fetching all attributes
		AttributeList expectedAttributeList =
				createAttributeList(ALL_ATTRIBUTE_NAMES_ARRAY, new Object[]{1, 23L, 1, 35L, 1, 51L});
		assertEquals(expectedAttributeList, mbean.getAttributes(ALL_ATTRIBUTE_NAMES_ARRAY));

		// check fetching subset of attributes
		String[] subsetOfNames = new String[]{SIMPLE_STATS_SUM, SIMPLE_STATS_COUNT, GROUPED_STATS_COUNTER_ONE_SUM};
		expectedAttributeList = createAttributeList(subsetOfNames, new Object[]{51L, 1, 23L});
		assertEquals(expectedAttributeList, mbean.getAttributes(subsetOfNames));

		// modify monitorable object
		monitorable.getGroupedStats().getCounterOne().recordValue(102L);
		// we don't write anything to groupedStats_counterTwo
		monitorable.getSimpleStats().recordValue(207L);

		// check fetching all attributes after modification
		expectedAttributeList =
				createAttributeList(ALL_ATTRIBUTE_NAMES_ARRAY, new Object[]{2, 23L + 102L, 1, 35L, 2, 51L + 207L});
		assertEquals(expectedAttributeList, mbean.getAttributes(ALL_ATTRIBUTE_NAMES_ARRAY));

		// check fetching subset of attributes after modification
		expectedAttributeList = createAttributeList(subsetOfNames, new Object[]{51L + 207L, 2, 23L + 102L});
		assertEquals(expectedAttributeList, mbean.getAttributes(subsetOfNames));
	}

	@Test
	public void itShouldAccumulateJmxStatsValuesFromSeveralMonitorable() throws Exception {
		MonitorableStub monitorable_1 = new MonitorableStub();
		MonitorableStub monitorable_2 = new MonitorableStub();
		monitorable_1.getSimpleStats().recordValue(23L);
		monitorable_2.getSimpleStats().recordValue(78L);

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		assertEquals(2, (int) mbean.getAttribute(SIMPLE_STATS_COUNT));
		assertEquals(23L + 78L, (long) mbean.getAttribute(SIMPLE_STATS_SUM));

		monitorable_1.getSimpleStats().recordValue(198L);
		monitorable_2.getSimpleStats().recordValue(201L);
		monitorable_2.getSimpleStats().recordValue(352L);

		assertEquals(5, (int) mbean.getAttribute(SIMPLE_STATS_COUNT));
		assertEquals(23L + 78L + 198L + 201L + 352L, (long) mbean.getAttribute(SIMPLE_STATS_SUM));
	}

	// helpers
	public static AttributeList createAttributeList(String[] names, Object[] values) {
		assert values.length == names.length;
		AttributeList attrList = new AttributeList();

		for (int i = 0; i < values.length; i++) {
			attrList.add(new Attribute(names[i], values[i]));
		}
		return attrList;
	}

	public static class MonitorableStub implements ConcurrentJmxMBean {
		CompositeStatsStub groupedStats = new CompositeStatsStub();
		JmxStatsStub simpleStats = new JmxStatsStub();

		@JmxAttribute
		public CompositeStatsStub getGroupedStats() {
			return groupedStats;
		}

		@JmxAttribute
		public JmxStatsStub getSimpleStats() {
			return simpleStats;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}
}
