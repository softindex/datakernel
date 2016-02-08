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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JmxMBeansPOJOAttributeTest {

	@Test
	public void itShouldCorrectlyComposeMBeanInfoInCaseOfPojoWithSimpleAttrs() throws Exception {
		PojoWithSimpleAttributes pojo = new PojoWithSimpleAttributes(150, "first");
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new MBeanWithPojoWithSimpleAttributes(pojo)), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}
		Collections.sort(attributeNames);

		assertEquals(attributeNames, asList("pojo_count", "pojo_name"));

	}

	@Test
	public void itShouldCorrectlyComposeMBeanInfoInCaseOfPojoWithAnotherPojo() throws Exception {
		PojoWithSimpleAttributes pojoOne = new PojoWithSimpleAttributes(150, "first");
		PojoWithSimpleAttributes pojoTwo = new PojoWithSimpleAttributes(230, "second");
		PojoWithAnotherPojos complexPojo = new PojoWithAnotherPojos(pojoOne, pojoTwo);
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(asList(new MBeanWithPojoWithAnotherPojo(complexPojo)), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}
		Collections.sort(attributeNames);

		assertEquals(attributeNames,
				asList("pojo_pojoOne_count", "pojo_pojoOne_name", "pojo_pojoTwo_count", "pojo_pojoTwo_name"));

	}

	// helpers

	public static class PojoWithSimpleAttributes {
		private int count;
		private String name;

		public PojoWithSimpleAttributes(int count, String name) {
			this.count = count;
			this.name = name;
		}

		@JmxAttribute
		public int getCount() {
			return count;
		}

		@JmxAttribute
		public String getName() {
			return name;
		}
	}

	public static class PojoWithAnotherPojos {
		private PojoWithSimpleAttributes pojoOne;
		private PojoWithSimpleAttributes pojoTwo;

		public PojoWithAnotherPojos(PojoWithSimpleAttributes pojoOne, PojoWithSimpleAttributes pojoTwo) {
			this.pojoOne = pojoOne;
			this.pojoTwo = pojoTwo;
		}

		@JmxAttribute
		public PojoWithSimpleAttributes getPojoOne() {
			return pojoOne;
		}

		@JmxAttribute
		public PojoWithSimpleAttributes getPojoTwo() {
			return pojoTwo;
		}
	}

	public static class MBeanWithPojoWithSimpleAttributes implements ConcurrentJmxMBean {
		private PojoWithSimpleAttributes pojo;

		public MBeanWithPojoWithSimpleAttributes(PojoWithSimpleAttributes pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithSimpleAttributes getPojo() {
			return pojo;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static class MBeanWithPojoWithAnotherPojo implements ConcurrentJmxMBean {
		private PojoWithAnotherPojos pojo;

		public MBeanWithPojoWithAnotherPojo(PojoWithAnotherPojos pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithAnotherPojos getPojo() {
			return pojo;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}
}
