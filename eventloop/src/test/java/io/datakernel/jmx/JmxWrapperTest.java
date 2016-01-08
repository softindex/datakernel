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
import com.sun.xml.internal.fastinfoset.stax.EventLocation;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.*;
import javax.management.openmbean.SimpleType;
import java.lang.management.ManagementFactory;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JmxWrapperTest {

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

		// sorted alphabetically
		List<String> expectedAttributeNames = asList(
				"groupedStats_counterOne_count",
				"groupedStats_counterOne_sum",
				"groupedStats_counterTwo_count",
				"groupedStats_counterTwo_sum",
				"simpleStats_count",
				"simpleStats_sum"
		);

		assertEquals(expectedAttributeNames, attributeNames);
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

}
