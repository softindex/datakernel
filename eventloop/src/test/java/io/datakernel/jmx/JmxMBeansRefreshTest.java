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
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.*;
import java.util.*;
import java.util.concurrent.Executor;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

// TODO(vmykhalko): start here. design and implement refreshing

public class JmxMBeansRefreshTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private final JmxStats<?> stats = context.mock(JmxStats.class);

	private final Eventloop eventloop = new Eventloop();
	private final MonitorableWithEventloop monitorable = new MonitorableWithEventloop(stats, eventloop);

	@Test
	public void itShouldCreateAttributesForRefreshControlIfRefreshIsEnabled()
			throws Exception {
		context.checking(new Expectations() {{
			// return empty map
			allowing(stats).getAttributes();
			will(returnValue(new TreeMap()));
		}});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), true);

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
	public void itShouldCreateOperationForRefreshControlIfRefreshIsEnabled() throws Exception {
		context.checking(new Expectations() {{
			// return empty map
			allowing(stats).getAttributes();
			will(returnValue(new TreeMap()));
		}});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), true);

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
		assertEquals("double", periodParameter.getType());
		assertEquals("period", periodParameter.getName());

		MBeanOperationInfo setSmoothingWindowOp = mBeanOperationsInfo[1];
		assertEquals("_setSmoothingWindow", setSmoothingWindowOp.getName());
		assertEquals("void", setSmoothingWindowOp.getReturnType());
		assertEquals(1, setSmoothingWindowOp.getSignature().length);
		MBeanParameterInfo smoothingWindowParameter = setSmoothingWindowOp.getSignature()[0];
		assertEquals("double", smoothingWindowParameter.getType());
		assertEquals("window", smoothingWindowParameter.getName());
	}

	@Test
	public void itShouldNotCreateAttributesAndOperationsForRefreshControlIfRefreshIsDisabled()
			throws Exception {
		context.checking(new Expectations() {{
			// return empty map
			allowing(stats).getAttributes();
			will(returnValue(new TreeMap()));
		}});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributesInfo = mBeanInfo.getAttributes();
		MBeanOperationInfo[] mBeanOperationsInfo = mBeanInfo.getOperations();

		assertEquals(Collections.emptyList(), asList(mBeanAttributesInfo));
		assertEquals(Collections.emptyList(), asList(mBeanOperationsInfo));
	}

	@Test
	public void itShouldProperlySetAndGetValuesForRefreshControl() throws Exception {
		context.checking(new Expectations() {{
			// return empty map
			allowing(stats).getAttributes();
			will(returnValue(new TreeMap()));
		}});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), true);

		double acceptableError = 1E-10;

		assertEquals(JmxMBeans.DEFAULT_REFRESH_PERIOD, (double) mbean.getAttribute("_refreshPeriod"), acceptableError);
		assertEquals(JmxMBeans.DEFAULT_SMOOTHING_WINDOW,
				(double) mbean.getAttribute("_smoothingWindow"), acceptableError);

		double newPeriod = 0.5;
		mbean.invoke("_setRefreshPeriod", new Object[]{newPeriod}, new String[]{"double"});

		assertEquals(newPeriod, (double) mbean.getAttribute("_refreshPeriod"), acceptableError);

		double newWindow = 3.5;
		mbean.invoke("_setSmoothingWindow", new Object[]{newWindow}, new String[]{"double"});

		assertEquals(newWindow, (double) mbean.getAttribute("_smoothingWindow"), acceptableError);
	}

	public static final class MonitorableWithEventloop implements ConcurrentJmxMBean {
		private final JmxStats<?> jmxStats;
		private final Eventloop eventloop;

		public MonitorableWithEventloop(JmxStats<?> jmxStats, Eventloop eventloop) {
			this.jmxStats = jmxStats;
			this.eventloop = eventloop;
		}

		@JmxAttribute
		public JmxStats<?> getJmxStats() {
			return jmxStats;
		}

		@Override
		public Executor getJmxExecutor() {
			return eventloop;
		}
	}
}
