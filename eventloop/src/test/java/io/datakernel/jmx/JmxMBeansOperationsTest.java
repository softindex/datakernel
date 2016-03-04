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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class JmxMBeansOperationsTest {

	@Test
	public void itShouldCollectInformationAbountJMXOperationsToMBeanInfo() throws Exception {
		MonitorableStubWithOperations monitorable = new MonitorableStubWithOperations();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
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
	public void itShouldInvokeAnnotanedOperationsThroughDynamicMBeanInterface() throws Exception {
		MonitorableStubWithOperations monitorable = new MonitorableStubWithOperations();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		mbean.invoke("increment", null, null);
		mbean.invoke("increment", null, null);

		mbean.invoke("addInfo", new Object[]{"data1"}, new String[]{String.class.getName()});
		mbean.invoke("addInfo", new Object[]{"data2"}, new String[]{String.class.getName()});

		mbean.invoke("multiplyAndAdd", new Object[]{120, 150}, new String[]{"long", "long"});

		assertEquals(monitorable.getCount(), 2);
		assertEquals(monitorable.getInfo(), "data1data2");
		assertEquals(monitorable.getSum(), 120 * 150);
	}

	@Test
	public void itShouldBroadcastOperationCallToAllMonitorables() throws Exception {
		MonitorableStubWithOperations monitorable_1 = new MonitorableStubWithOperations();
		MonitorableStubWithOperations monitorable_2 = new MonitorableStubWithOperations();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		// set manually init value for second monitorable to be different from first
		monitorable_2.inc();
		monitorable_2.inc();
		monitorable_2.inc();
		monitorable_2.addInfo("second");
		monitorable_2.multiplyAndAdd(10, 15);

		mbean.invoke("increment", null, null);
		mbean.invoke("increment", null, null);

		mbean.invoke("addInfo", new Object[]{"data1"}, new String[]{String.class.getName()});
		mbean.invoke("addInfo", new Object[]{"data2"}, new String[]{String.class.getName()});

		mbean.invoke("multiplyAndAdd", new Object[]{120, 150}, new String[]{"long", "long"});

		// check first monitorable
		assertEquals(monitorable_1.getCount(), 2);
		assertEquals(monitorable_1.getInfo(), "data1data2");
		assertEquals(monitorable_1.getSum(), 120 * 150);

		// check second monitorable
		assertEquals(monitorable_2.getCount(), 2 + 3);
		assertEquals(monitorable_2.getInfo(), "second" + "data1data2");
		assertEquals(monitorable_2.getSum(), 10 * 15 + 120 * 150);
	}

	@Test
	public void operationReturnsValueInCaseOfSingleMBeanInPool() throws Exception {
		MBeanWithOperationThatReturnsValue mbeanOpWithValue = new MBeanWithOperationThatReturnsValue();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(mbeanOpWithValue), false);

		assertEquals(15, (int)mbean.invoke("sum", new Object[]{7, 8}, new String[]{"int", "int"}));
	}

	@Test
	public void operationReturnsNullInCaseOfSeveralMBeansInPool() throws Exception {
		MBeanWithOperationThatReturnsValue mbeanOpWithValue_1 = new MBeanWithOperationThatReturnsValue();
		MBeanWithOperationThatReturnsValue mbeanOpWithValue_2 = new MBeanWithOperationThatReturnsValue();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(mbeanOpWithValue_1, mbeanOpWithValue_2), false);

		assertEquals(null, mbean.invoke("sum", new Object[]{7, 8}, new String[]{"int", "int"}));
	}

	// helpers
	public static class MonitorableStubWithOperations implements ConcurrentJmxMBean {
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
		public void addInfo(@JmxParameter("information") String info) {
			this.info += info;
		}

		@JmxOperation
		public void multiplyAndAdd(long valueOne, long valueTwo) {
			sum += valueOne * valueTwo;
		}

		@Override
		public Executor getJmxExecutor() {
			return new Executor() {
				@Override
				public void execute(Runnable command) {
					command.run();
				}
			};
		}
	}

	public static final class MBeanWithOperationThatReturnsValue implements ConcurrentJmxMBean {

		@JmxOperation
		public int sum(int a, int b) {
			return a + b;
		}

		@Override
		public Executor getJmxExecutor() {
			return new Executor() {
				@Override
				public void execute(Runnable command) {
					command.run();
				}
			};
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
