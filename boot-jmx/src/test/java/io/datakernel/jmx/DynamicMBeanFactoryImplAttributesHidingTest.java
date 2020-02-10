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
import io.datakernel.eventloop.jmx.EventloopJmxMBean;
import io.datakernel.eventloop.jmx.JmxRefreshableStats;
import io.datakernel.jmx.api.JmxAttribute;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.List;
import java.util.Map;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.jmx.MBeanSettings.defaultSettings;
import static io.datakernel.jmx.helper.Utils.nameToAttribute;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicMBeanFactoryImplAttributesHidingTest {
	private static final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

	// region pojos
	@Test
	public void omitsNullPojoAttributesInSingleton() {
		MBeanStubOne singletonWithNullPojo = new MBeanStubOne(null);
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(singletonList(singletonWithNullPojo), defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void omitsPojoAttributesInWorkersIfAllWorkersReturnNull() {
		MBeanStubOne mbean_1 = new MBeanStubOne(null);
		MBeanStubOne mbean_2 = new MBeanStubOne(null);
		List<MBeanStubOne> workersWithNullPojo = asList(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void doesNotOmitPojoAttributesInWorkersIfAtLeastOneWorkerReturnsNonNull() {
		MBeanStubOne mbean_1 = new MBeanStubOne(null);
		MBeanStubOne mbean_2 = new MBeanStubOne(new PojoStub());
		List<MBeanStubOne> workersWithNullPojo = asList(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(2, attrs.size());
		assertTrue(attrs.containsKey("pojoStub_number"));
		assertTrue(attrs.containsKey("pojoStub_text"));
	}

	public static final class PojoStub {
		@JmxAttribute
		public int getNumber() {
			return 5;
		}

		@JmxAttribute
		public String getText() {
			return "text";
		}
	}

	public static final class MBeanStubOne implements EventloopJmxMBean {
		private final PojoStub pojoStub;

		public MBeanStubOne(PojoStub pojoStub) {
			this.pojoStub = pojoStub;
		}

		@JmxAttribute
		public PojoStub getPojoStub() {
			return pojoStub;
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return eventloop;
		}
	}
	// endregion

	// region jmx stats
	@Test
	public void omitsNullJmxStatsAttributesInSingleton() {
		MBeanStubTwo singletonWithNullJmxStats = new MBeanStubTwo(null);
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(singletonList(singletonWithNullJmxStats), defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void omitsJmxStatsAttributesInWorkersIfAllWorkersReturnNull() {
		MBeanStubTwo mbean_1 = new MBeanStubTwo(null);
		MBeanStubTwo mbean_2 = new MBeanStubTwo(null);
		List<MBeanStubTwo> workersWithNullPojo = asList(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void doesNotOmitJmxStatsAttributesInWorkersIfAtLeastOneWorkerReturnsNonNull() {
		MBeanStubTwo mbean_1 = new MBeanStubTwo(null);
		MBeanStubTwo mbean_2 = new MBeanStubTwo(new JmxStatsStub());
		List<MBeanStubTwo> workersWithNullPojo = asList(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(1, attrs.size());
		assertTrue(attrs.containsKey("jmxStatsStub_value"));
	}

	public static final class JmxStatsStub implements JmxRefreshableStats<JmxStatsStub> {
		@JmxAttribute
		public double getValue() {
			return 5;
		}

		@Override
		public void refresh(long timestamp) {
		}

		@Override
		public void add(JmxStatsStub another) {
		}
	}

	public static final class MBeanStubTwo implements EventloopJmxMBean {
		private final JmxStatsStub jmxStatsStub;

		public MBeanStubTwo(JmxStatsStub jmxStatsStub) {
			this.jmxStatsStub = jmxStatsStub;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStatsStub() {
			return jmxStatsStub;
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return eventloop;
		}
	}
	// endregion

	// region stats in pojo
	@Test
	public void omitsNullPojosInNonNullPojos() {
		MBeanStubThree monitorable = new MBeanStubThree(new PojoStubThree(null));
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(singletonList(monitorable), defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(1, attrs.size());
		assertTrue(attrs.containsKey("pojoStubThree_number"));
	}

	public static final class PojoStubThree {
		private final JmxStatsStub jmxStatsStub;

		public PojoStubThree(JmxStatsStub jmxStatsStub) {
			this.jmxStatsStub = jmxStatsStub;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStats() {
			return jmxStatsStub;
		}

		@JmxAttribute
		public int getNumber() {
			return 10;
		}
	}

	public static final class MBeanStubThree implements EventloopJmxMBean {
		private final PojoStubThree pojoStubThree;

		public MBeanStubThree(PojoStubThree pojoStubThree) {
			this.pojoStubThree = pojoStubThree;
		}

		@JmxAttribute
		public PojoStubThree getPojoStubThree() {
			return pojoStubThree;
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return eventloop;
		}
	}
	// endregion
}
