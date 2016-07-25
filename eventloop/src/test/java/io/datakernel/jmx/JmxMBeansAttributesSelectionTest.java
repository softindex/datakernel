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
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.jmx.JmxMBeansAttributesTest.createDynamicMBeanFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxMBeansAttributesSelectionTest {

	@Test
	public void doNotConsiderOptionalAttributesByDefault() throws Exception {
		MBeanWithNoExtraSubAttributes mbeanStub = new MBeanWithNoExtraSubAttributes();
		DynamicMBean mbean = createDynamicMBeanFor(mbeanStub);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		assertEquals(1, attributesInfoArr.length);
		assertEquals("stats_sum", attributesInfoArr[0].getName());

	}

	@Test
	public void considerOptionalAttributesIfTheyAreSpecified() throws Exception {
		MBeanWithExtraSubAttributes mbeanStub = new MBeanWithExtraSubAttributes();
		DynamicMBean mbean = createDynamicMBeanFor(mbeanStub);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(2, nameToAttr.size());
		assertTrue(nameToAttr.containsKey("stats_sum"));
		assertTrue(nameToAttr.containsKey("stats_count"));

	}

	@Test(expected = RuntimeException.class)
	public void throwsExceptionInCaseOfInvalidFieldName() throws Exception {
		MBeansStubWithInvalidExtraAttrName mbeanStub = new MBeansStubWithInvalidExtraAttrName();
		DynamicMBean mbean = createDynamicMBeanFor(mbeanStub);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		assertEquals(1, attributesInfoArr.length);
		assertEquals("stats_sum", attributesInfoArr[0].getName());
	}

	public static Map<String, MBeanAttributeInfo> nameToAttribute(MBeanAttributeInfo[] attrs) {
		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
		for (MBeanAttributeInfo attr : attrs) {
			nameToAttr.put(attr.getName(), attr);
		}
		return nameToAttr;
	}

	public static class MBeanWithNoExtraSubAttributes implements EventloopJmxMBean {
		private final JmxStatsWithOptionalAttributes stats = new JmxStatsWithOptionalAttributes();

		@JmxAttribute
		public JmxStatsWithOptionalAttributes getStats() {
			return stats;
		}

		@Override
		public Eventloop getEventloop() {
			return null;
		}
	}

	public static class MBeanWithExtraSubAttributes implements EventloopJmxMBean {
		private final JmxStatsWithOptionalAttributes stats = new JmxStatsWithOptionalAttributes();

		@JmxAttribute(extraSubAttributes = {"count"})
		public JmxStatsWithOptionalAttributes getStats() {
			return stats;
		}

		@Override
		public Eventloop getEventloop() {
			return null;
		}
	}

	public static class MBeansStubWithInvalidExtraAttrName implements EventloopJmxMBean {
		private final JmxStatsWithOptionalAttributes stats = new JmxStatsWithOptionalAttributes();

		@JmxAttribute(extraSubAttributes = {"QWERTY"}) // QWERTY subAttribute doesn't exist
		public JmxStatsWithOptionalAttributes getStats() {
			return stats;
		}

		@Override
		public Eventloop getEventloop() {
			return null;
		}
	}

	public static class JmxStatsWithOptionalAttributes implements JmxRefreshableStats<JmxStatsWithOptionalAttributes> {

		private long sum = 0L;
		private int count = 0;

		public void recordValue(long value) {
			sum += value;
			++count;
		}

		@JmxAttribute
		public long getSum() {
			return sum;
		}

		@JmxAttribute(optional = true)
		public int getCount() {
			return count;
		}

		@Override
		public void add(JmxStatsWithOptionalAttributes stats) {
			this.sum += stats.sum;
			this.count += stats.count;
		}

		@Override
		public void refresh(long timestamp) {

		}
	}
}
