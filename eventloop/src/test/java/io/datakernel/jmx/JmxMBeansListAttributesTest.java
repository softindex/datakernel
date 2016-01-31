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
import javax.management.openmbean.*;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxMBeansListAttributesTest {

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
