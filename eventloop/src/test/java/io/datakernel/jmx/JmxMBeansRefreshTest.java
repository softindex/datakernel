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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.jmx.annotation.JmxMBean;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.*;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JmxMBeansRefreshTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private final JmxStats<?> stats = context.mock(JmxStats.class);

	private final Eventloop eventloop = new NioEventloop();
	private final MonitorableWithEventloop monitorable = new MonitorableWithEventloop(stats, eventloop);

	@Test
	public void itShouldCreateAttributesForRefreshControlIfMonitorableHasEventloopGetter()
			throws Exception {
		context.checking(new Expectations() {{
			// return empty map
			allowing(stats).getAttributes();
			will(returnValue(new TreeMap()));
		}});

		DynamicMBean mbean = JmxMBeans.factory().createFor(monitorable);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributesInfo = mBeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributesInfo) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}
		Collections.sort(attributeNames);

		// there are no any other attributes except refreshPeriod and smoothingWindow
		List<String> expectedAttributeNames = asList("_refreshPeriod", "_smoothingWindow");
		assertEquals(expectedAttributeNames, attributeNames);
	}

	@Test
	public void itShouldCreateOperationForRefreshControlIfMonitorableHasEventloopGetter() throws Exception {
		context.checking(new Expectations() {{
			// return empty map
			allowing(stats).getAttributes();
			will(returnValue(new TreeMap()));
		}});

		DynamicMBean mbean = JmxMBeans.factory().createFor(monitorable);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanOperationInfo[] mBeanOperationsInfo = mBeanInfo.getOperations();
		Arrays.sort(mBeanOperationsInfo, new Comparator<MBeanOperationInfo>() {
			@Override
			public int compare(MBeanOperationInfo op1, MBeanOperationInfo op2) {
				return op1.getName().compareTo(op2.getName());
			}
		});

		// there are no any other operations except setPeriod and setSmoothingWindow
		MBeanOperationInfo setPeriodOp = mBeanOperationsInfo[0];
		assertEquals("_setRefreshPeriod", setPeriodOp.getName());
		assertEquals("void", setPeriodOp.getReturnType());
		assertEquals(1, setPeriodOp.getSignature().length);
		MBeanParameterInfo periodParameter = setPeriodOp.getSignature()[0];
		assertEquals("int", periodParameter.getType());
		assertEquals("period", periodParameter.getName());

		MBeanOperationInfo setSmoothingWindowOp = mBeanOperationsInfo[1];
		assertEquals("_setSmoothingWindow", setSmoothingWindowOp.getName());
		assertEquals("void", setSmoothingWindowOp.getReturnType());
		assertEquals(1, setSmoothingWindowOp.getSignature().length);
		MBeanParameterInfo smoothingWindowParameter = setSmoothingWindowOp.getSignature()[0];
		assertEquals("double", smoothingWindowParameter.getType());
		assertEquals("window", smoothingWindowParameter.getName());
	}

	@JmxMBean
	public static final class MonitorableWithEventloop {
		private final JmxStats<?> jmxStats;
		private final Eventloop eventloop;

		public MonitorableWithEventloop(JmxStats<?> jmxStats, Eventloop eventloop) {
			this.jmxStats = jmxStats;
			this.eventloop = eventloop;
		}

		public JmxStats<?> getJmxStats() {
			return jmxStats;
		}

		public Eventloop getEventloop() {
			return eventloop;
		}
	}
}
