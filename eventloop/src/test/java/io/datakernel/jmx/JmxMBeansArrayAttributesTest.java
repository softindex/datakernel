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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JmxMBeansArrayAttributesTest {

	@Test
	public void itShouldCollectInfoAboutArrayAttributes() throws Exception {
		MonitorableWithArray monitorable =
				new MonitorableWithArray(new SimplePOJO[]{new SimplePOJO("data-10"), new SimplePOJO("data-20")});
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> nameToAttr = formNameToAttr(mBeanInfo.getAttributes());
		assertEquals(1, nameToAttr.size());

		MBeanAttributeInfo listAttr = nameToAttr.get("arrayAttr");
		// array of objects is represented as array of strings using toString() method
		assertEquals(String[].class.getName(), listAttr.getType());
		assertEquals(true, listAttr.isReadable());
		assertEquals(false, listAttr.isWritable());
	}

	@Test
	public void itShouldProperlyReturnsList() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("data-10");
		SimplePOJO pojo_2 = new SimplePOJO("data-20");
		MonitorableWithArray monitorable =
				new MonitorableWithArray(new SimplePOJO[]{pojo_1, pojo_2});
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		String[] expected = new String[]{pojo_1.toString(), pojo_2.toString()};
		String[] actual = (String[]) mbean.getAttribute("arrayAttr");
		assertArrayEquals(expected, actual);
	}

	@Test
	public void itShouldProperlyAggregateLists() throws Exception {
		SimplePOJO pojo_1 = new SimplePOJO("data-10");
		SimplePOJO pojo_2 = new SimplePOJO("data-20");
		MonitorableWithArray monitorable_1 =
				new MonitorableWithArray(new SimplePOJO[]{pojo_1, pojo_2});

		SimplePOJO pojo_3 = new SimplePOJO("data-500");
		MonitorableWithArray monitorable_2 =
				new MonitorableWithArray(new SimplePOJO[]{pojo_3});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2));

		Set<String> expected = new HashSet<>(asList((String[]) mbean.getAttribute("arrayAttr")));
		Set<String> actual = new HashSet<>(asList(pojo_1.toString(), pojo_2.toString(), pojo_3.toString()));
		assertEquals(expected, actual);
	}

	@Test
	public void itShouldIgnoreNullArrayss() throws Exception {
		MonitorableWithArray monitorable_1 =
				new MonitorableWithArray(null);

		SimplePOJO pojo_1 = new SimplePOJO("data-500");
		MonitorableWithArray monitorable_2 =
				new MonitorableWithArray(new SimplePOJO[]{pojo_1});

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2));

		Set<String> expected = new HashSet<>(asList((String[]) mbean.getAttribute("arrayAttr")));
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
	public static final class MonitorableWithArray {
		private SimplePOJO[] arrayAttr;

		public MonitorableWithArray(SimplePOJO[] arrayAttr) {
			this.arrayAttr = arrayAttr;
		}

		@JmxAttribute
		public SimplePOJO[] getArrayAttr() {
			return arrayAttr;
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
