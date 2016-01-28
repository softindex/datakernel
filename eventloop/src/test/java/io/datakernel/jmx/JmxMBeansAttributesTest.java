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

import javax.management.*;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JmxMBeansAttributesTest {

	@Test
	public void itShouldCollectAllJmxAttributesOfBasicTypes() throws Exception {
		MonitorableWithIntegerAttribute monitorable =
				new MonitorableWithIntegerAttribute(true, 100, 1000L, 1.5, "data");
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> nameToAttr = formNameToAttr(mBeanInfo.getAttributes());
		assertEquals(5, nameToAttr.size());

		// read-only-attribute
		MBeanAttributeInfo booleanAttr = nameToAttr.get("booleanAttr");
		assertEquals("boolean", booleanAttr.getType());
		assertEquals(true, booleanAttr.isReadable());
		assertEquals(false, booleanAttr.isWritable());

		// read-write-attribute
		MBeanAttributeInfo intAttr = nameToAttr.get("intAttr");
		assertEquals("int", intAttr.getType());
		assertEquals(true, intAttr.isReadable());
		assertEquals(true, intAttr.isWritable());

		// read-only-attribute
		MBeanAttributeInfo longAttr = nameToAttr.get("longAttr");
		assertEquals("long", longAttr.getType());
		assertEquals(true, longAttr.isReadable());
		assertEquals(false, longAttr.isWritable());

		// read-only-attribute
		MBeanAttributeInfo doubleAttr = nameToAttr.get("doubleAttr");
		assertEquals("double", doubleAttr.getType());
		assertEquals(true, doubleAttr.isReadable());
		assertEquals(false, doubleAttr.isWritable());

		// read-write-attribute
		MBeanAttributeInfo strAttr = nameToAttr.get("strAttr");
		assertEquals("java.lang.String", strAttr.getType());
		assertEquals(true, strAttr.isReadable());
		assertEquals(true, strAttr.isWritable());
	}

	@Test
	public void itShouldReturnCommonValueIfAllValuesAreSameForPoolOfMonitorables() throws Exception {
		MonitorableWithIntegerAttribute monitorable_1 =
				new MonitorableWithIntegerAttribute(true, 100, 1000L, 1.5, "data");
		MonitorableWithIntegerAttribute monitorable_2 =
				new MonitorableWithIntegerAttribute(true, 100, 1000L, 1.5, "data");
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2));

		assertEquals(true, mbean.getAttribute("booleanAttr"));
		assertEquals(100, mbean.getAttribute("intAttr"));
		assertEquals(1000L, mbean.getAttribute("longAttr"));
		assertEquals(1.5, mbean.getAttribute("doubleAttr"));
		assertEquals("data", mbean.getAttribute("strAttr"));
	}

	@Test
	public void itShouldThrowExceptionIfValuesDifferInInstancesOfPoolOfMonitorables() throws Exception {
		MonitorableWithIntegerAttribute monitorable_1 =
				new MonitorableWithIntegerAttribute(true, 100, 1000L, 1.5, "data");
		MonitorableWithIntegerAttribute monitorable_2 =
				new MonitorableWithIntegerAttribute(false, 250, 25000L, 5.0, "data-2");
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2));

		int exceptionsThrown = 0;

		try {
			mbean.getAttribute("booleanAttr");
		} catch (MBeanException mbeanException) {
			exceptionsThrown++;
		}

		try {
			mbean.getAttribute("intAttr");
		} catch (MBeanException mbeanException) {
			exceptionsThrown++;
		}

		try {
			mbean.getAttribute("longAttr");
		} catch (MBeanException mbeanException) {
			exceptionsThrown++;
		}

		try {
			mbean.getAttribute("doubleAttr");
		} catch (MBeanException mbeanException) {
			exceptionsThrown++;
		}

		try {
			mbean.getAttribute("strAttr");
		} catch (MBeanException mbeanException) {
			exceptionsThrown++;
		}

		assertEquals(5, exceptionsThrown);
	}

	@Test
	public void itShouldSetWritableAttributes() throws Exception {
		MonitorableWithIntegerAttribute monitorable =
				new MonitorableWithIntegerAttribute(true, 100, 1000L, 1.5, "data");
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		mbean.setAttribute(new Attribute("intAttr", 320));
		mbean.setAttribute(new Attribute("strAttr", "message"));

		assertEquals(320, mbean.getAttribute("intAttr"));
		assertEquals("message", mbean.getAttribute("strAttr"));
	}

	@Test
	public void itShouldNotSetNonReadOnlyAttributes() throws Exception {
		MonitorableWithIntegerAttribute monitorable =
				new MonitorableWithIntegerAttribute(true, 100, 1000L, 1.5, "data");
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		int exceptions = 0;

		try {
			mbean.setAttribute(new Attribute("booleanAttr", false));
		} catch (AttributeNotFoundException e) {
			exceptions++;
		}

		try {
			mbean.setAttribute(new Attribute("longAttr", 800L));
		} catch (AttributeNotFoundException e) {
			exceptions++;
		}

		try {
			mbean.setAttribute(new Attribute("doubleAttr", 17.3));
		} catch (AttributeNotFoundException e) {
			exceptions++;
		}

		assertEquals(3, exceptions);
		assertEquals(true, mbean.getAttribute("booleanAttr"));
		assertEquals(1000L, mbean.getAttribute("longAttr"));
		assertEquals(1.5, mbean.getAttribute("doubleAttr"));
	}

	@Test
	public void itShouldCollectInfoAboutListAttributes() throws Exception {
		MonitorableWithList monitorable =
				new MonitorableWithList(asList(new SimplePOJO("data-10"), new SimplePOJO("data-20")));
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> nameToAttr = formNameToAttr(mBeanInfo.getAttributes());
		assertEquals(1, nameToAttr.size());

		MBeanAttributeInfo listAttr = nameToAttr.get("listAttr");
		// list of objects is represented as array of strings using toString() method
		assertEquals(String[].class.getName(), listAttr.getType());
		assertEquals(true, listAttr.isReadable());
		assertEquals(false, listAttr.isWritable());
	}

	@Test
	public void itShouldProperlyReturnsList() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("data-10");
		SimplePOJO pojo_2 = new SimplePOJO("data-20");
		MonitorableWithList monitorable =
				new MonitorableWithList(asList(pojo_1, pojo_2));
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		String[] expected = new String[]{pojo_1.toString(), pojo_2.toString()};
		String[] actual = (String[]) mbean.getAttribute("listAttr");
		assertArrayEquals(expected, actual);
	}

	@Test
	public void itShouldProperlyAggregateLists() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("data-10");
		SimplePOJO pojo_2 = new SimplePOJO("data-20");
		MonitorableWithList monitorable_1 =
				new MonitorableWithList(asList(pojo_1, pojo_2));

		SimplePOJO pojo_3 = new SimplePOJO("data-500");
		MonitorableWithList monitorable_2 =
				new MonitorableWithList(asList(pojo_3));

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2));

		Set<String> expected = new HashSet<>(asList((String[]) mbean.getAttribute("listAttr")));
		Set<String> actual = new HashSet<>(asList(pojo_1.toString(), pojo_2.toString(), pojo_3.toString()));
		assertEquals(expected, actual);
	}

	@Test
	public void itShouldIgnoreNullLists() throws Exception {
		MonitorableWithList monitorable_1 =
				new MonitorableWithList(null);

		SimplePOJO pojo_1 = new SimplePOJO("data-500");
		MonitorableWithList monitorable_2 =
				new MonitorableWithList(asList(pojo_1));

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2));

		Set<String> expected = new HashSet<>(asList((String[]) mbean.getAttribute("listAttr")));
		Set<String> actual = new HashSet<>(asList(pojo_1.toString()));
		assertEquals(expected, actual);
	}

	// helpers
	public static Map<String, MBeanAttributeInfo> formNameToAttr(MBeanAttributeInfo[] attributes) {
		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
		for (MBeanAttributeInfo attribute : attributes) {
			nameToAttr.put(attribute.getName(), attribute);
		}
		return nameToAttr;
	}

	@JmxMBean
	public static final class MonitorableWithIntegerAttribute {
		private boolean booleanAttr;
		private int intAttr;
		private long longAttr;
		private double doubleAttr;
		private String strAttr;

		public MonitorableWithIntegerAttribute(boolean booleanAttr, int intAttr, long longAttr,
		                                       double doubleAttr, String strAttr) {
			this.booleanAttr = booleanAttr;
			this.intAttr = intAttr;
			this.longAttr = longAttr;
			this.doubleAttr = doubleAttr;
			this.strAttr = strAttr;
		}

		@JmxAttribute
		public boolean getBooleanAttr() {
			return booleanAttr;
		}

		@JmxAttribute
		public int getIntAttr() {
			return intAttr;
		}

		@JmxAttribute
		public long getLongAttr() {
			return longAttr;
		}

		@JmxAttribute
		public double getDoubleAttr() {
			return doubleAttr;
		}

		@JmxAttribute
		public String getStrAttr() {
			return strAttr;
		}

		// intAttr and strAttr are writable
		@JmxAttribute
		public void setIntAttr(int intAttr) {
			this.intAttr = intAttr;
		}

		@JmxAttribute
		public void setStrAttr(String strAttr) {
			this.strAttr = strAttr;
		}
	}

	@JmxMBean
	public static final class MonitorableWithList {
		private List<SimplePOJO> listAttr;

		public MonitorableWithList(List<SimplePOJO> listAttr) {
			this.listAttr = listAttr;
		}

		@JmxAttribute
		public List<SimplePOJO> getListAttr() {
			return listAttr;
		}
	}

	public static final class SimplePOJO {
		private final String name;

		public SimplePOJO(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "name=" + name;
		}
	}

}
