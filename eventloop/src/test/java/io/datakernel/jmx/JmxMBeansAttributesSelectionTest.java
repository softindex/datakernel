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
import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

import static io.datakernel.jmx.JmxMBeansAttributesTest.createDynamicMBeanFor;
import static org.junit.Assert.assertEquals;

public class JmxMBeansAttributesSelectionTest {

	@Test
	public void considersOnlySpecifiedFields() throws Exception {
		MBeanStub mbeanStub = new MBeanStub();
		DynamicMBean mbean = createDynamicMBeanFor(mbeanStub);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		assertEquals(1, attributesInfoArr.length);
		assertEquals("stats_sum", attributesInfoArr[0].getName());

	}

	@Test(expected = RuntimeException.class)
	public void throwsExceptionInCaseOfInvalidFieldName() throws Exception {
		MBeansStubWithInvalidFieldName mbeanStub = new MBeansStubWithInvalidFieldName();
		DynamicMBean mbean = createDynamicMBeanFor(mbeanStub);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		assertEquals(1, attributesInfoArr.length);
		assertEquals("stats_sum", attributesInfoArr[0].getName());
	}

	public static final class MBeanStub implements EventloopJmxMBean {
		private final JmxStatsStub stats = new JmxStatsStub();

		@JmxAttribute(fields = {"sum"})
		public JmxStatsStub getStats() {
			return stats;
		}

		@Override
		public Eventloop getEventloop() {
			return null;
		}
	}

	public static final class MBeansStubWithInvalidFieldName implements EventloopJmxMBean {
		private final JmxStatsStub stats = new JmxStatsStub();

		@JmxAttribute(fields = {"sum", "QWERTY"}) // QWERTY field doesn't exist
		public JmxStatsStub getStats() {
			return stats;
		}

		@Override
		public Eventloop getEventloop() {
			return null;
		}
	}
}
