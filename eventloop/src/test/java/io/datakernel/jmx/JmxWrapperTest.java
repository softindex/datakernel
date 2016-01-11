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

import com.sun.glass.ui.EventLoop;
import io.datakernel.jmx.annotation.JmxNamedParameter;
import io.datakernel.jmx.annotation.JmxOperation;
import io.datakernel.jmx.helper.JmxStatsStub;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import javax.management.*;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class JmxWrapperTest {
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
	public void itShouldCollectAllJmxStatsFromMonitorableAndWriteThemToMBeanInfo() {
		MonitorableStub monitorable = new MonitorableStub();
		JmxWrapper wrapper = JmxWrapper.wrap(monitorable);

		MBeanInfo mBeanInfo = wrapper.getMBeanInfo();
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
	public void itShouldFetchSingleAttributeValueCorrectly() throws MBeanException, AttributeNotFoundException, ReflectionException {
		MonitorableStub monitorable = new MonitorableStub();
		monitorable.getGroupedStats().getCounterOne().recordValue(23L);
		monitorable.getGroupedStats().getCounterTwo().recordValue(35L);
		monitorable.getSimpleStats().recordValue(51L);
		JmxWrapper wrapper = JmxWrapper.wrap(monitorable);

		// check init values of attributes
		assertEquals(1, (int) wrapper.getAttribute(GROUPED_STATS_COUNTER_ONE_COUNT));
		assertEquals(23L, (long) wrapper.getAttribute(GROUPED_STATS_COUNTER_ONE_SUM));
		assertEquals(1, (int) wrapper.getAttribute(GROUPED_STATS_COUNTER_TWO_COUNT));
		assertEquals(35L, (long) wrapper.getAttribute(GROUPED_STATS_COUNTER_TWO_SUM));
		assertEquals(1, (int) wrapper.getAttribute(SIMPLE_STATS_COUNT));
		assertEquals(51L, (long) wrapper.getAttribute(SIMPLE_STATS_SUM));

		// modify monitorable object
		monitorable.getGroupedStats().getCounterOne().recordValue(102L);
		// we don't write anything to groupedStats_counterTwo
		monitorable.getSimpleStats().recordValue(207L);

		// check modified values of attributes
		assertEquals(2, (int) wrapper.getAttribute(GROUPED_STATS_COUNTER_ONE_COUNT));
		assertEquals(23L + 102L, (long) wrapper.getAttribute(GROUPED_STATS_COUNTER_ONE_SUM));
		assertEquals(1, (int) wrapper.getAttribute(GROUPED_STATS_COUNTER_TWO_COUNT));
		assertEquals(35L, (long) wrapper.getAttribute(GROUPED_STATS_COUNTER_TWO_SUM));
		assertEquals(2, (int) wrapper.getAttribute(SIMPLE_STATS_COUNT));
		assertEquals(51L + 207L, (long) wrapper.getAttribute(SIMPLE_STATS_SUM));
	}

	@Test
	public void itShouldFetchBunchOfAttributeValuesCorrectly() {
		MonitorableStub monitorable = new MonitorableStub();
		monitorable.getGroupedStats().getCounterOne().recordValue(23L);
		monitorable.getGroupedStats().getCounterTwo().recordValue(35L);
		monitorable.getSimpleStats().recordValue(51L);
		JmxWrapper wrapper = JmxWrapper.wrap(monitorable);

		// check fetching all attributes
		AttributeList expectedAttributeList =
				createAttributeList(ALL_ATTRIBUTE_NAMES_ARRAY, new Object[]{1, 23L, 1, 35L, 1, 51L});
		assertEquals(expectedAttributeList, wrapper.getAttributes(ALL_ATTRIBUTE_NAMES_ARRAY));

		// check fetching subset of attributes
		String[] subsetOfNames = new String[]{SIMPLE_STATS_SUM, SIMPLE_STATS_COUNT, GROUPED_STATS_COUNTER_ONE_SUM};
		expectedAttributeList = createAttributeList(subsetOfNames, new Object[]{51L, 1, 23L});
		assertEquals(expectedAttributeList, wrapper.getAttributes(subsetOfNames));

		// modify monitorable object
		monitorable.getGroupedStats().getCounterOne().recordValue(102L);
		// we don't write anything to groupedStats_counterTwo
		monitorable.getSimpleStats().recordValue(207L);

		// check fetching all attributes after modification
		expectedAttributeList =
				createAttributeList(ALL_ATTRIBUTE_NAMES_ARRAY, new Object[]{2, 23L + 102L, 1, 35L, 2, 51L + 207L});
		assertEquals(expectedAttributeList, wrapper.getAttributes(ALL_ATTRIBUTE_NAMES_ARRAY));

		// check fetching subset of attributes after modification
		expectedAttributeList = createAttributeList(subsetOfNames, new Object[]{51L + 207L, 2, 23L + 102L});
		assertEquals(expectedAttributeList, wrapper.getAttributes(subsetOfNames));
	}

	@Test
	public void itShouldCollectInformationAbountJMXOperationsToMBeanInfo() {
		MonitorableStubWithOperations monitorable = new MonitorableStubWithOperations();
		JmxWrapper wrapper = JmxWrapper.wrap(monitorable);

		MBeanInfo mBeanInfo = wrapper.getMBeanInfo();
		MBeanOperationInfo[] operations = mBeanInfo.getOperations();
		Map<String, MBeanOperationInfo> nameToOperation = new HashMap<>();
		for (MBeanOperationInfo operation : operations) {
			nameToOperation.put(operation.getName(), operation);
		}

		assertThat(nameToOperation, hasKey("increment"));
		assertThat(nameToOperation, hasKey("addInfo"));
		assertThat(nameToOperation, hasKey("multiplyAndAdd"));

		MBeanOperationInfo incrementOperation = nameToOperation.get("increment");
		MBeanOperationInfo addInfoOperation = nameToOperation.get("addInfo");
		MBeanOperationInfo multiplyAndAddOperation = nameToOperation.get("multiplyAndAdd");

		assertThat(incrementOperation, hasReturnType("void"));

		assertThat(addInfoOperation, hasParameter("information", String.class.getName()));
		assertThat(addInfoOperation, hasReturnType("void"));

		// parameter names are not annotated
		assertThat(multiplyAndAddOperation, hasParameter("arg0", "long"));
		assertThat(multiplyAndAddOperation, hasParameter("arg1", "long"));
		assertThat(multiplyAndAddOperation, hasReturnType("void"));
	}

	@Test
	public void itShouldInvokeAnnotanedOperationsThroughDynamicMBeanInterface() throws ReflectionException, MBeanException {
		MonitorableStubWithOperations monitorable = new MonitorableStubWithOperations();
		JmxWrapper wrapper = JmxWrapper.wrap(monitorable);

		wrapper.invoke("increment", null, null);
		wrapper.invoke("increment", null, null);

		wrapper.invoke("addInfo", new Object[]{"data1"}, new String[]{String.class.getName()});
		wrapper.invoke("addInfo", new Object[]{"data2"}, new String[]{String.class.getName()});

		wrapper.invoke("multiplyAndAdd", new Object[]{120, 150}, new String[]{"long", "long"});

		assertEquals(monitorable.getCount(), 2);
		assertEquals(monitorable.getInfo(), "data1data2");
		assertEquals(monitorable.getSum(), 120 * 150);
	}

	// TODO(vmykhalko): add test for methods with same names but different signatures

	// helpers
	public static AttributeList createAttributeList(String[] names, Object[] values) {
		assert values.length == names.length;
		AttributeList attrList = new AttributeList();

		for (int i = 0; i < values.length; i++) {
			attrList.add(new Attribute(names[i], values[i]));
		}
		return attrList;
	}

//	@Test
//	public void test() {
//		System.out.println(SimpleType.INTEGER.getTypeName());
//	}

	public static class MonitorableStub implements JmxMonitorable {
		CompositeStatsStub groupedStats = new CompositeStatsStub();
		JmxStatsStub simpleStats = new JmxStatsStub();

		@Override
		public EventLoop getEventloop() {
			return null;
		}

		public CompositeStatsStub getGroupedStats() {
			return groupedStats;
		}

		public JmxStatsStub getSimpleStats() {
			return simpleStats;
		}
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

	public static class MonitorableStubWithOperations implements JmxMonitorable {

		private int count = 0;
		private String info = "";
		private long sum = 0;

		public int getCount() {
			return count;
		}

		public String getInfo() {
			return info;
		}

		public long getSum() {
			return sum;
		}

		@JmxOperation(name = "increment")
		public void inc() {
			count++;
		}

		@JmxOperation
		public void addInfo(@JmxNamedParameter("information") String info) {
			this.info += info;
		}

		@JmxOperation
		public void multiplyAndAdd(long valueOne, long valueTwo) {
			sum += valueOne * valueTwo;
		}

		@Override
		public EventLoop getEventloop() {
			return null;
		}
	}

	// custom matchers
	public static <T> Matcher<Map<T, ?>> hasKey(final T key) {
		return new BaseMatcher<Map<T, ?>>() {

			@Override
			public void describeTo(Description description) {
				description.appendText("has key \"" + key.toString() + "\"");
			}

			@Override
			public boolean matches(Object item) {
				if (item == null) {
					return false;
				}
				Map<T, ?> map = (Map<T, ?>) item;
				return map.containsKey(key);
			}
		};
	}

	public static Matcher<MBeanOperationInfo> hasParameter(final String name, final String type) {
		return new BaseMatcher<MBeanOperationInfo>() {
			@Override
			public boolean matches(Object item) {
				if (item == null) {
					return false;
				}
				MBeanOperationInfo operation = (MBeanOperationInfo) item;
				for (MBeanParameterInfo param : operation.getSignature()) {
					if (param.getName().equals(name) && param.getType().equals(type)) {
						return true;
					}
				}
				return false;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("has parameter with name \"" + name + "\" and type \"" + type + "\"");
			}
		};
	}

	public static Matcher<MBeanOperationInfo> hasReturnType(final String type) {
		return new BaseMatcher<MBeanOperationInfo>() {
			@Override
			public boolean matches(Object item) {
				if (item == null) {
					return false;
				}
				MBeanOperationInfo operation = (MBeanOperationInfo) item;
				return operation.getReturnType().equals(type);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("has return type " + type);
			}
		};
	}

}
