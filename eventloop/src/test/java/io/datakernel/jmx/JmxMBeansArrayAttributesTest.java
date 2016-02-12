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
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JmxMBeansArrayAttributesTest {

	@Test
	public void itShouldCollectInfoAboutArrayAttributes() throws Exception {
		MonitorableWithArrayOfIntegers monitorable =
				new MonitorableWithArrayOfIntegers(new Integer[]{10, 25});
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> nameToAttr = formNameToAttr(mBeanInfo.getAttributes());
		assertEquals(1, nameToAttr.size());

		MBeanAttributeInfo listAttr = nameToAttr.get("intArr");
		// list of objects is represented as array of strings using toString() method
		assertEquals(String[].class.getName(), listAttr.getType());
		assertEquals(true, listAttr.isReadable());
		assertEquals(false, listAttr.isWritable());
	}

	@Test
	public void itShouldProperlyReturnsArrayOfSimpleElements() throws Exception {
		MonitorableWithArrayOfIntegers monitorable =
				new MonitorableWithArrayOfIntegers(new Integer[]{10, 25});
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		Integer[] expected = new Integer[]{10, 25};
		Integer[] actual = (Integer[]) mbean.getAttribute("intArr");
		assertArrayEquals(expected, actual);
	}

	@Test
	public void itShouldProperlyAggregateListsOfSimpleElements() throws Exception {
		MonitorableWithArrayOfIntegers monitorable_1 =
				new MonitorableWithArrayOfIntegers(new Integer[]{10, 25});

		MonitorableWithArrayOfIntegers monitorable_2 =
				new MonitorableWithArrayOfIntegers(new Integer[]{157});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		Integer[] expected = new Integer[]{10, 25, 157};
		Integer[] actual = (Integer[]) mbean.getAttribute("intArr");
		assertArrayEquals(expected, actual);
	}

	@Test
	public void itShouldProperlyReturnsArrayOfPojos() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("John", 28);
		SimplePOJO pojo_2 = new SimplePOJO("Nick", 23);
		MonitorableWithArrayOfPojos monitorable = new MonitorableWithArrayOfPojos(new SimplePOJO[]{pojo_1, pojo_2});
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);

		CompositeData[] actualPojos = (CompositeData[]) mbean.getAttribute("pojoArr");

		assertEquals(2, actualPojos.length);

		assertEquals("John", actualPojos[0].get("name"));
		assertEquals(28, actualPojos[0].get("count"));

		assertEquals("Nick", actualPojos[1].get("name"));
		assertEquals(23, actualPojos[1].get("count"));
	}

	@Test
	public void itShouldProperlyAggregateArraysOfPojos() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("John", 28);
		SimplePOJO pojo_2 = new SimplePOJO("Nick", 23);
		MonitorableWithArrayOfPojos monitorable_1 = new MonitorableWithArrayOfPojos(new SimplePOJO[]{pojo_1, pojo_2});

		SimplePOJO pojo_3 = new SimplePOJO("Sam", 25);
		MonitorableWithArrayOfPojos monitorable_2 =
				new MonitorableWithArrayOfPojos(new SimplePOJO[]{pojo_3});
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		CompositeData[] actualPojos = (CompositeData[]) mbean.getAttribute("pojoArr");

		assertEquals(3, actualPojos.length);

		assertEquals("John", actualPojos[0].get("name"));
		assertEquals(28, actualPojos[0].get("count"));

		assertEquals("Nick", actualPojos[1].get("name"));
		assertEquals(23, actualPojos[1].get("count"));

		assertEquals("Sam", actualPojos[2].get("name"));
		assertEquals(25, actualPojos[2].get("count"));
	}

	@Test
	public void itShouldIgnoreNullArrays() throws Exception {
		MonitorableWithArrayOfPojos monitorable_1 =
				new MonitorableWithArrayOfPojos(null);

		SimplePOJO pojo_1 = new SimplePOJO("Jack", 50);
		MonitorableWithArrayOfPojos monitorable_2 =
				new MonitorableWithArrayOfPojos(new SimplePOJO[]{pojo_1});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);

		CompositeData[] actualPojos = (CompositeData[]) mbean.getAttribute("pojoArr");

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

	public static final class MonitorableWithArrayOfPojos implements ConcurrentJmxMBean {
		private SimplePOJO[] pojoArr;

		public MonitorableWithArrayOfPojos(SimplePOJO[] pojoArr) {
			this.pojoArr = pojoArr;
		}

		@JmxAttribute
		public SimplePOJO[] getPojoArr() {
			return pojoArr;
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

	public static final class MonitorableWithArrayOfIntegers implements ConcurrentJmxMBean {
		private Integer[] intArr;

		public MonitorableWithArrayOfIntegers(Integer[] intArr) {
			this.intArr = intArr;
		}

		@JmxAttribute
		public Integer[] getIntArr() {
			return intArr;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}
}
