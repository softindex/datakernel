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

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.openmbean.CompositeData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JmxMBeansListAttributesTest {

	@Test
	public void itShouldCollectInfoAboutListAttributes() throws Exception {
		MonitorableWithListOfIntegers monitorable =
				new MonitorableWithListOfIntegers(asList(10, 25));
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

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
	public void itShouldProperlyReturnsListOfSimpleElements() throws Exception {
		MonitorableWithListOfIntegers monitorable =
				new MonitorableWithListOfIntegers(asList(10, 25));
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		Integer[] expected = new Integer[]{10, 25};
		Integer[] actual = (Integer[]) mbean.getAttribute("listAttr");
		assertArrayEquals(expected, actual);
	}

	@Test
	public void itShouldProperlyAggregateListsOfSimpleElements() throws Exception {
		MonitorableWithListOfIntegers monitorable_1 =
				new MonitorableWithListOfIntegers(asList(10, 25));

		MonitorableWithListOfIntegers monitorable_2 =
				new MonitorableWithListOfIntegers(asList(157));

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		Integer[] expected = new Integer[]{10, 25, 157};
		Integer[] actual = (Integer[]) mbean.getAttribute("listAttr");
		assertArrayEquals(expected, actual);
	}

	@Test
	public void itShouldProperlyReturnsListOfPojos() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("John", 28);
		SimplePOJO pojo_2 = new SimplePOJO("Nick", 23);
		MonitorableWithListOfPojo monitorable =
				new MonitorableWithListOfPojo(asList(pojo_1, pojo_2));
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		CompositeData[] actualPojos = (CompositeData[]) mbean.getAttribute("listAttr");

		assertEquals(2, actualPojos.length);

		assertEquals("John", actualPojos[0].get("name"));
		assertEquals(28, actualPojos[0].get("count"));

		assertEquals("Nick", actualPojos[1].get("name"));
		assertEquals(23, actualPojos[1].get("count"));
	}

	@Test
	public void itShouldProperlyAggregateListsOfPojos() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("John", 28);
		SimplePOJO pojo_2 = new SimplePOJO("Nick", 23);
		MonitorableWithListOfPojo monitorable_1 =
				new MonitorableWithListOfPojo(asList(pojo_1, pojo_2));

		SimplePOJO pojo_3 = new SimplePOJO("Sam", 25);
		MonitorableWithListOfPojo monitorable_2 =
				new MonitorableWithListOfPojo(asList(pojo_3));
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		CompositeData[] actualPojos = (CompositeData[]) mbean.getAttribute("listAttr");

		assertEquals(3, actualPojos.length);

		assertEquals("John", actualPojos[0].get("name"));
		assertEquals(28, actualPojos[0].get("count"));

		assertEquals("Nick", actualPojos[1].get("name"));
		assertEquals(23, actualPojos[1].get("count"));

		assertEquals("Sam", actualPojos[2].get("name"));
		assertEquals(25, actualPojos[2].get("count"));
	}

	@Test
	public void itShouldIgnoreNullLists() throws Exception {
		MonitorableWithListOfPojo monitorable_1 =
				new MonitorableWithListOfPojo(null);

		SimplePOJO pojo_1 = new SimplePOJO("Jack", 50);
		MonitorableWithListOfPojo monitorable_2 =
				new MonitorableWithListOfPojo(asList(pojo_1));

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		CompositeData[] actualPojos = (CompositeData[]) mbean.getAttribute("listAttr");

		assertEquals(1, actualPojos.length);

		assertEquals("Jack", actualPojos[0].get("name"));
		assertEquals(50, actualPojos[0].get("count"));
	}

	// helpers
	public static Map<String, MBeanAttributeInfo> formNameToAttr(MBeanAttributeInfo[] attributes) {
		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
		for (MBeanAttributeInfo attribute : attributes) {
			nameToAttr.put(attribute.getName(), attribute);
		}
		return nameToAttr;
	}

	public static final class MonitorableWithListOfPojo implements ConcurrentJmxMBean {
		private List<SimplePOJO> listAttr;

		public MonitorableWithListOfPojo(List<SimplePOJO> listAttr) {
			this.listAttr = listAttr;
		}

		@JmxAttribute
		public List<SimplePOJO> getListAttr() {
			return listAttr;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static final class SimplePOJO {
		private final String name;
		private final int count;

		public SimplePOJO(String name, int count) {
			this.name = name;
			this.count = count;
		}

		@JmxAttribute
		public String getName() {
			return name;
		}

		@JmxAttribute
		public int getCount() {
			return count;
		}
	}

	public static final class MonitorableWithListOfIntegers implements ConcurrentJmxMBean {
		private List<Integer> list;

		public MonitorableWithListOfIntegers(List<Integer> list) {
			this.list = list;
		}

		@JmxAttribute
		public List<Integer> getListAttr() {
			return list;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}
}
