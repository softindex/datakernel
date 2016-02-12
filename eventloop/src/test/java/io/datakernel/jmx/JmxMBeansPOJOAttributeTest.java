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

import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.openmbean.CompositeData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class JmxMBeansPOJOAttributeTest {

	@Test
	public void itShouldCorrectlyFetchAttrsInCaseOfPojoWithSimpleAttrs() throws Exception {
		PojoWithSimpleAttributes pojo = new PojoWithSimpleAttributes(150, "first");
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new MBeanWithPojoWithSimpleAttributes(pojo)), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}
		Collections.sort(attributeNames);

		assertEquals(asList("pojo_count", "pojo_name"), attributeNames);

		assertEquals(150, mbean.getAttribute("pojo_count"));
		assertEquals("first", mbean.getAttribute("pojo_name"));
	}

	@Test
	public void itShouldProperlyAccumulateAttrsInCaseOfPojoWithSimpleAttrs() throws Exception {
		PojoWithSimpleAttributes pojo_1 = new PojoWithSimpleAttributes(150, "first");
		PojoWithSimpleAttributes pojo_2 = new PojoWithSimpleAttributes(200, new String("first"));

		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(
						asList(
								new MBeanWithPojoWithSimpleAttributes(pojo_1),
								new MBeanWithPojoWithSimpleAttributes(pojo_2)),
						false);

		// string attrs are equal
		assertEquals("first", mbean.getAttribute("pojo_name"));

		// int attrs are not equal
		int exceptionsThrown = 0;
		try {
			mbean.getAttribute("pojo_count");
		} catch (MBeanException e) {
			exceptionsThrown++;
		}
		assertEquals(1, exceptionsThrown);
	}

	@Test
	public void itShouldCorrectlyFetchAttrsInCaseOfPojoWithAnotherPojo() throws Exception {
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

		assertEquals(asList("pojo_pojoOne_count", "pojo_pojoOne_name", "pojo_pojoTwo_count", "pojo_pojoTwo_name"),
				attributeNames);

		assertEquals(150, mbean.getAttribute("pojo_pojoOne_count"));
		assertEquals("first", mbean.getAttribute("pojo_pojoOne_name"));
		assertEquals(230, mbean.getAttribute("pojo_pojoTwo_count"));
		assertEquals("second", mbean.getAttribute("pojo_pojoTwo_name"));

	}

	@Test
	public void itShouldProperlyAccumulateAttrsInCaseOfPojoWithAnotherPojo() throws Exception {
		PojoWithSimpleAttributes pojoOne_1 = new PojoWithSimpleAttributes(150, "first");
		PojoWithSimpleAttributes pojoTwo_1 = new PojoWithSimpleAttributes(230, "second");
		PojoWithAnotherPojos complexPojo_1 = new PojoWithAnotherPojos(pojoOne_1, pojoTwo_1);

		PojoWithSimpleAttributes pojoOne_2 = new PojoWithSimpleAttributes(150, "different-str");
		PojoWithSimpleAttributes pojoTwo_2 = new PojoWithSimpleAttributes(10000, "second");
		PojoWithAnotherPojos complexPojo_2 = new PojoWithAnotherPojos(pojoOne_2, pojoTwo_2);

		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(
						asList(
								new MBeanWithPojoWithAnotherPojo(complexPojo_1),
								new MBeanWithPojoWithAnotherPojo(complexPojo_2)
						),
						false);

		// string attrs are equal
		assertEquals(150, mbean.getAttribute("pojo_pojoOne_count"));
		assertEquals("second", mbean.getAttribute("pojo_pojoTwo_name"));

		// int attrs are not equal
		int exceptionsThrown = 0;
		try {
			mbean.getAttribute("pojo_pojoOne_name");
		} catch (MBeanException e) {
			exceptionsThrown++;
		}
		try {
			mbean.getAttribute("pojo_pojoTwo_count");
		} catch (MBeanException e) {
			exceptionsThrown++;
		}
		assertEquals(2, exceptionsThrown);

	}

	@Test
	public void itShouldCorrectlyFetchAttrsInCaseOfPojoWithList() throws Exception {
		PojoWithList pojo = new PojoWithList(asList("one", "two"));
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(asList(new MBeanWithPojoWithList(pojo)), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}

		assertEquals(asList("pojo_list"), attributeNames);

		assertArrayEquals(new Object[]{"one", "two"}, (Object[]) mbean.getAttribute("pojo_list"));
	}

	@Test
	public void itShouldProperlyAccumulateAttrsInCaseOfPojoWithList() throws Exception {
		PojoWithList pojo_1 = new PojoWithList(asList("one", "two"));
		PojoWithList pojo_2 = new PojoWithList(asList("ten", "nine", "eight"));
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(asList(new MBeanWithPojoWithList(pojo_1), new MBeanWithPojoWithList(pojo_2)), false);

		assertArrayEquals(
				new Object[]{"one", "two", "ten", "nine", "eight"},
				(Object[]) mbean.getAttribute("pojo_list")
		);
	}

	@Test
	public void itShouldCorrectlyFetchAttrsInCaseOfPojoWithArray() throws Exception {
		PojoWithArray pojo = new PojoWithArray(new String[]{"one", "two"});
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(asList(new MBeanWithPojoWithArray(pojo)), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}

		assertEquals(asList("pojo_array"), attributeNames);

		assertArrayEquals(new Object[]{"one", "two"}, (Object[]) mbean.getAttribute("pojo_array"));
	}

	@Test
	public void itShouldProperlyAccumulateAttrsInCaseOfPojoWithArray() throws Exception {
		PojoWithArray pojo_1 = new PojoWithArray(new String[]{"one", "two"});
		PojoWithArray pojo_2 = new PojoWithArray(new String[]{"ten", "eleven", "twelve"});
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(asList(new MBeanWithPojoWithArray(pojo_1), new MBeanWithPojoWithArray(pojo_2)), false);

		assertArrayEquals(
				new Object[]{"one", "two", "ten", "eleven", "twelve"},
				(Object[]) mbean.getAttribute("pojo_array")
		);
	}

	@Test
	public void itShouldCorrectlyFetchAttrsInCaseOfPojoWithThrowable() throws Exception {
		Exception exception = new RuntimeException("msg");
		PojoWithThrowable pojo = new PojoWithThrowable(exception);
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(asList(new MBeanWithPojoWithThrowable(pojo)), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}

		assertEquals(asList("pojo_throwable"), attributeNames);

		assertTrue(mbean.getAttribute("pojo_throwable") instanceof CompositeData);
		assertEquals("msg", ((CompositeData) mbean.getAttribute("pojo_throwable")).get("message"));
	}

	@Test
	public void itShouldProperlyAccumulateAttrsInCaseOfPojoWithThrowable() throws Exception {
		Exception exception = new RuntimeException("msg");
		PojoWithThrowable pojo_1 = new PojoWithThrowable(exception);
		PojoWithThrowable pojo_2 = new PojoWithThrowable(exception);
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(
						asList(
								new MBeanWithPojoWithThrowable(pojo_1),
								new MBeanWithPojoWithThrowable(pojo_2)),
						false);

		assertTrue(mbean.getAttribute("pojo_throwable") instanceof CompositeData);
		assertEquals("msg", ((CompositeData) mbean.getAttribute("pojo_throwable")).get("message"));
	}

	@Test
	public void itShouldCorrectlyFetchAttrsInCaseOfPojoWithJmxStats() throws Exception {
		JmxStatsStub jmxStats = new JmxStatsStub();
		jmxStats.recordValue(1500);
		PojoWithJmxStats pojo = new PojoWithJmxStats(jmxStats);
		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(asList(new MBeanWithPojoWithJmxStats(pojo)), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes();
		List<String> attributeNames = new ArrayList<>();
		for (MBeanAttributeInfo mBeanAttributeInfo : mBeanAttributeInfos) {
			attributeNames.add(mBeanAttributeInfo.getName());
		}
		Collections.sort(attributeNames);

		assertEquals(asList("pojo_jmxStats_count", "pojo_jmxStats_sum"), attributeNames);

		assertEquals(1500L, mbean.getAttribute("pojo_jmxStats_sum"));
		assertEquals(1, mbean.getAttribute("pojo_jmxStats_count"));
	}

	@Test
	public void itShouldProperlyAccumulateAttrsInCaseOfPojoWithJmxStats() throws Exception {
		JmxStatsStub jmxStats_1 = new JmxStatsStub();
		jmxStats_1.recordValue(1500);
		PojoWithJmxStats pojo_1 = new PojoWithJmxStats(jmxStats_1);

		JmxStatsStub jmxStats_2 = new JmxStatsStub();
		jmxStats_2.recordValue(2000);
		PojoWithJmxStats pojo_2 = new PojoWithJmxStats(jmxStats_2);

		DynamicMBean mbean = JmxMBeans.factory()
				.createFor(
						asList(
								new MBeanWithPojoWithJmxStats(pojo_1),
								new MBeanWithPojoWithJmxStats(pojo_2)
						),
						false);

		assertEquals(3500L, mbean.getAttribute("pojo_jmxStats_sum"));
		assertEquals(2, mbean.getAttribute("pojo_jmxStats_count"));
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

	public static class PojoWithList {
		private List<String> list;

		public PojoWithList(List<String> list) {
			this.list = list;
		}

		@JmxAttribute
		public List<String> getList() {
			return list;
		}
	}

	public static class MBeanWithPojoWithList implements ConcurrentJmxMBean {
		private PojoWithList pojo;

		public MBeanWithPojoWithList(PojoWithList pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithList getPojo() {
			return pojo;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static class PojoWithArray {
		private String[] array;

		public PojoWithArray(String[] array) {
			this.array = array;
		}

		@JmxAttribute
		public String[] getArray() {
			return array;
		}
	}

	public static class MBeanWithPojoWithArray implements ConcurrentJmxMBean {
		private PojoWithArray pojo;

		public MBeanWithPojoWithArray(PojoWithArray pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithArray getPojo() {
			return pojo;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static class PojoWithThrowable {
		private Throwable throwable;

		public PojoWithThrowable(Throwable throwable) {
			this.throwable = throwable;
		}

		@JmxAttribute
		public Throwable getThrowable() {
			return throwable;
		}
	}

	public static class MBeanWithPojoWithThrowable implements ConcurrentJmxMBean {
		private PojoWithThrowable pojo;

		public MBeanWithPojoWithThrowable(PojoWithThrowable pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithThrowable getPojo() {
			return pojo;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static class PojoWithJmxStats {
		private JmxStatsStub jmxStats;

		public PojoWithJmxStats(JmxStatsStub jmxStats) {
			this.jmxStats = jmxStats;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStats() {
			return jmxStats;
		}
	}

	public static class MBeanWithPojoWithJmxStats implements ConcurrentJmxMBean {
		private PojoWithJmxStats pojo;

		public MBeanWithPojoWithJmxStats(PojoWithJmxStats pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithJmxStats getPojo() {
			return pojo;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}
}
