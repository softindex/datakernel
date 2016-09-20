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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class JmxMBeansAttributesTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void retreivesProperMBeanInfo() throws Exception {
		MBeanWithSimpleAttrsAndPojo mbeanOneSample = new MBeanWithSimpleAttrsAndPojo("data", new SamplePojo(5, 100));
		DynamicMBean mbean = createDynamicMBeanFor(mbeanOneSample);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(3, nameToAttr.size());

		assertTrue(nameToAttr.containsKey("info"));
		assertTrue(nameToAttr.get("info").isReadable());

		assertTrue(nameToAttr.containsKey("details_count"));
		assertTrue(nameToAttr.get("details_count").isReadable());

		assertTrue(nameToAttr.containsKey("details_sum"));
		assertTrue(nameToAttr.get("details_sum").isReadable());
	}

	@Test
	public void retreivesProperAttributeValues() throws Exception {
		MBeanWithSimpleAttrsAndPojo mbeanOneSample = new MBeanWithSimpleAttrsAndPojo("data", new SamplePojo(5, 100L));
		DynamicMBean mbean = createDynamicMBeanFor(mbeanOneSample);

		assertEquals("data", mbean.getAttribute("info"));
		assertEquals(5, mbean.getAttribute("details_count"));
		assertEquals(100L, mbean.getAttribute("details_sum"));
	}

	/*
	 * tests for aggregation
	 */
	@Test
	public void ifAppropriateAttributesInDifferentMBeansHaveSameValueReturnsThatValue() throws Exception {
		MBeanWithSingleIntAttr mbean_1 = new MBeanWithSingleIntAttr(1);
		MBeanWithSingleIntAttr mbean_2 = new MBeanWithSingleIntAttr(1);

		DynamicMBean mbean = createDynamicMBeanFor(mbean_1, mbean_2);

		assertEquals(1, mbean.getAttribute("value"));
	}

	@Test
	public void ifAppropriateAttributesInDifferentMBeansHaveDifferentValueReturnsNull() throws Exception {
		MBeanWithSingleIntAttr mbean_1 = new MBeanWithSingleIntAttr(1);
		MBeanWithSingleIntAttr mbean_2 = new MBeanWithSingleIntAttr(2);

		DynamicMBean mbean = createDynamicMBeanFor(mbean_1, mbean_2);

		assertEquals(null, mbean.getAttribute("value"));
	}

	@Test
	public void aggregatesJmxStatsUsingTheirAggregationPolicy() throws Exception {
		JmxStatsStub stats_1 = new JmxStatsStub();
		stats_1.recordValue(100);
		stats_1.recordValue(20);

		JmxStatsStub stats_2 = new JmxStatsStub();
		stats_2.recordValue(5);

		MBeanWithJmxStats mBeanWithJmxStats_1 = new MBeanWithJmxStats(stats_1);
		MBeanWithJmxStats mBeanWithJmxStats_2 = new MBeanWithJmxStats(stats_2);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithJmxStats_1, mBeanWithJmxStats_2);

		assertEquals(3, mbean.getAttribute("jmxStatsStub_count"));
		assertEquals(125L, mbean.getAttribute("jmxStatsStub_sum"));
	}

	@Test
	public void concatenatesListAttributesFromDifferentMBeans() throws Exception {
		MBeanWithListAttr mBeanWithListAttr_1 = new MBeanWithListAttr(asList("a", "b"));
		MBeanWithListAttr mBeanWithListAttr_2 = new MBeanWithListAttr(new ArrayList<String>());
		MBeanWithListAttr mBeanWithListAttr_3 = new MBeanWithListAttr(asList("w"));

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithListAttr_1, mBeanWithListAttr_2, mBeanWithListAttr_3);

		assertArrayEquals(new String[]{"a", "b", "w"}, (String[]) mbean.getAttribute("list"));
	}

	@Test
	public void properlyAggregateMapsByKeyAccordingToTheirValueAggregationPolicy() throws Exception {
		Map<String, Integer> map_1 = new HashMap<>();
		map_1.put("a", 1);
		map_1.put("b", 2);
		map_1.put("c", 100);

		Map<String, Integer> map_2 = new HashMap<>();
		map_2.put("b", 2);
		map_2.put("c", 200);
		map_2.put("d", 5);

		MBeanWithMap mBeanWithMap_1 = new MBeanWithMap(map_1);
		MBeanWithMap mBeanWithMap_2 = new MBeanWithMap(map_2);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithMap_1, mBeanWithMap_2);

		TabularData tabularData = (TabularData) mbean.getAttribute("nameToNumber");

		assertEquals(4, tabularData.size());

		CompositeData row_1 = tabularData.get(keyForTabularData("a"));
		assertEquals(1, row_1.get("value"));

		CompositeData row_2 = tabularData.get(keyForTabularData("b"));
		assertEquals(2, row_2.get("value"));

		CompositeData row_3 = tabularData.get(keyForTabularData("c"));
		assertEquals(null, row_3.get("value"));

		CompositeData row_4 = tabularData.get(keyForTabularData("d"));
		assertEquals(5, row_4.get("value"));
	}

	// test empty names
	@Test
	public void handlesEmptyAttributeNamesProperly() throws Exception {
		MBeanWithEmptyNames mBeanWithEmptyNames =
				new MBeanWithEmptyNames(
						new SamplePojo_L_1_1(10),
						new SamplePojo_L_1_2(
								new SamplePojo_L_2(25)
						)
				);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithEmptyNames);

		MBeanAttributeInfo[] attributesInfoArr = mbean.getMBeanInfo().getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(2, nameToAttr.size());

		assertTrue(nameToAttr.containsKey("group_count"));
		assertTrue(nameToAttr.containsKey("group_total"));

		assertEquals(10, mbean.getAttribute("group_count"));
		assertEquals(25, mbean.getAttribute("group_total"));
	}

	// test setters
	@Test
	public void returnsInfoAboutWritableAttributesInMBeanInfo() throws Exception {
		MBeanWithSettableAttributes settableMBean = new MBeanWithSettableAttributes(10, 20, "data");
		DynamicMBean mbean = createDynamicMBeanFor(settableMBean);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(3, nameToAttr.size());

		assertTrue(nameToAttr.containsKey("notSettableInt"));
		assertFalse(nameToAttr.get("notSettableInt").isWritable());

		assertTrue(nameToAttr.containsKey("settableInt"));
		assertTrue(nameToAttr.get("settableInt").isWritable());

		assertTrue(nameToAttr.containsKey("settableStr"));
		assertTrue(nameToAttr.get("settableStr").isWritable());
	}

	@Test
	public void setsWritableAttributes() throws Exception {
		MBeanWithSettableAttributes settableMBean = new MBeanWithSettableAttributes(10, 20, "data");
		DynamicMBean mbean = createDynamicMBeanFor(settableMBean);

		mbean.setAttribute(new Attribute("settableInt", 125));
		mbean.setAttribute(new Attribute("settableStr", "data-2"));

		assertEquals(125, mbean.getAttribute("settableInt"));
		assertEquals("data-2", mbean.getAttribute("settableStr"));
	}

	@Test
	public void setsWrittableAttributesInInnerPojo() throws Exception {
		PojoWithSettableAttribute pojo = new PojoWithSettableAttribute(20);
		MBeanWithPojoWithSettableAttribute settableMBean = new MBeanWithPojoWithSettableAttribute(pojo);
		DynamicMBean mbean = createDynamicMBeanFor(settableMBean);

		mbean.setAttribute(new Attribute("pojo_sum", 155L));

		assertEquals(155L, mbean.getAttribute("pojo_sum"));
	}

	@Test
	public void worksProperlyWithIsGetters() throws MBeanException, AttributeNotFoundException, ReflectionException {
		MBeanWithIsGetter mBeanWithIsGetter = new MBeanWithIsGetter();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(mBeanWithIsGetter), false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		assertEquals(1, attributesInfoArr.length);

		MBeanAttributeInfo booleanAttr = attributesInfoArr[0];
		assertEquals("active", booleanAttr.getName());
		assertTrue(booleanAttr.isReadable());
		assertTrue(booleanAttr.isIs());

		assertTrue((boolean) mbean.getAttribute("active"));
	}

	@Test
	public void returns_toString_representationOfObjectsThatAreNeitherSupportedTypesNorPojosWithJmxAttributes()
			throws MBeanException, AttributeNotFoundException, ReflectionException {

		ArbitraryType arbitraryType = new ArbitraryType("String representation");
		Date date = new Date();
		MBeanWithJmxAttributesOfArbitraryTypes obj =
				new MBeanWithJmxAttributesOfArbitraryTypes(arbitraryType, date);

		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(obj), false);

		assertEquals(arbitraryType.toString(), mbean.getAttribute("arbitraryType"));
		assertEquals(date.toString(), mbean.getAttribute("date"));
	}

	/*
	 * helper methods
 	 */

	public static DynamicMBean createDynamicMBeanFor(Object... objects) throws Exception {
		boolean refreshEnabled = false;
		return JmxMBeans.factory().createFor(asList(objects), refreshEnabled);
	}

	public static Map<String, MBeanAttributeInfo> nameToAttribute(MBeanAttributeInfo[] attrs) {
		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
		for (MBeanAttributeInfo attr : attrs) {
			nameToAttr.put(attr.getName(), attr);
		}
		return nameToAttr;
	}

	public static Object[] keyForTabularData(String key) {
		return new String[]{key};
	}

	/*
	 * helper classes
	 */

	public static final class SamplePojo {
		private final int count;
		private final long sum;

		public SamplePojo(int count, long sum) {
			this.count = count;
			this.sum = sum;
		}

		@JmxAttribute
		public int getCount() {
			return count;
		}

		@JmxAttribute
		public long getSum() {
			return sum;
		}
	}

	public static final class MBeanWithSimpleAttrsAndPojo implements ConcurrentJmxMBean {
		private final String info;
		private final SamplePojo samplePojo;

		public MBeanWithSimpleAttrsAndPojo(String info, SamplePojo samplePojo) {
			this.info = info;
			this.samplePojo = samplePojo;
		}

		@JmxAttribute
		public String getInfo() {
			return info;
		}

		@JmxAttribute(name = "details")
		public SamplePojo getSamplePojo() {
			return samplePojo;
		}
	}

	public static final class MBeanWithListAttr implements EventloopJmxMBean {
		private final List<String> list;

		public MBeanWithListAttr(List<String> list) {
			this.list = list;
		}

		@JmxAttribute
		public List<String> getList() {
			return list;
		}

		@Override
		public Eventloop getEventloop() {
			return Eventloop.create();
		}
	}

	public static final class MBeanWithSingleIntAttr implements EventloopJmxMBean {
		private final int value;

		public MBeanWithSingleIntAttr(int value) {
			this.value = value;
		}

		@JmxAttribute
		public int getValue() {
			return value;
		}

		@Override
		public Eventloop getEventloop() {
			return Eventloop.create();
		}
	}

	public static final class MBeanWithJmxStats implements EventloopJmxMBean {
		private final JmxStatsStub jmxStatsStub;

		public MBeanWithJmxStats(JmxStatsStub jmxStatsStub) {
			this.jmxStatsStub = jmxStatsStub;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStatsStub() {
			return jmxStatsStub;
		}

		@Override
		public Eventloop getEventloop() {
			return Eventloop.create();
		}
	}

	public static final class MBeanWithMap implements EventloopJmxMBean {
		private final Map<String, Integer> nameToNumber;

		public MBeanWithMap(Map<String, Integer> nameToNumber) {
			this.nameToNumber = nameToNumber;
		}

		@JmxAttribute
		public Map<String, Integer> getNameToNumber() {
			return nameToNumber;
		}

		@Override
		public Eventloop getEventloop() {
			return Eventloop.create();
		}
	}

	// classes for empty names tests
	public static final class MBeanWithEmptyNames implements ConcurrentJmxMBean {
		private final SamplePojo_L_1_1 pojo1;
		private final SamplePojo_L_1_2 pojo2;

		public MBeanWithEmptyNames(SamplePojo_L_1_1 pojo1, SamplePojo_L_1_2 pojo2) {
			this.pojo1 = pojo1;
			this.pojo2 = pojo2;
		}

		@JmxAttribute(name = "group")
		public SamplePojo_L_1_1 getPojo1() {
			return pojo1;
		}

		@JmxAttribute(name = "")
		public SamplePojo_L_1_2 getPojo2() {
			return pojo2;
		}
	}

	public static final class SamplePojo_L_1_1 {
		private final int count;

		public SamplePojo_L_1_1(int count) {
			this.count = count;
		}

		@JmxAttribute(name = "count")
		public int getCount() {
			return count;
		}
	}

	public static final class SamplePojo_L_1_2 {
		private final SamplePojo_L_2 pojo;

		public SamplePojo_L_1_2(SamplePojo_L_2 pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute(name = "group")
		public SamplePojo_L_2 getPojo() {
			return pojo;
		}
	}

	public static final class SamplePojo_L_2 {
		private final int total;

		public SamplePojo_L_2(int total) {
			this.total = total;
		}

		@JmxAttribute(name = "total")
		public int getTotal() {
			return total;
		}
	}

	// classes for setter tests
	public static final class MBeanWithSettableAttributes implements ConcurrentJmxMBean {
		private final int notSettableInt;
		private int settableInt;
		private String settableStr;

		public MBeanWithSettableAttributes(int notSettableInt, int settableInt, String settableStr) {
			this.notSettableInt = notSettableInt;
			this.settableInt = settableInt;
			this.settableStr = settableStr;
		}

		@JmxAttribute
		public int getNotSettableInt() {
			return notSettableInt;
		}

		@JmxAttribute
		public int getSettableInt() {
			return settableInt;
		}

		@JmxAttribute
		public String getSettableStr() {
			return settableStr;
		}

		@JmxAttribute
		public void setSettableInt(int settableInt) {
			this.settableInt = settableInt;
		}

		@JmxAttribute
		public void setSettableStr(String settableStr) {
			this.settableStr = settableStr;
		}
	}

	public static final class MBeanWithPojoWithSettableAttribute implements ConcurrentJmxMBean {
		private final PojoWithSettableAttribute pojo;

		public MBeanWithPojoWithSettableAttribute(PojoWithSettableAttribute pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithSettableAttribute getPojo() {
			return pojo;
		}
	}

	public static final class PojoWithSettableAttribute {
		private long sum;

		public PojoWithSettableAttribute(long sum) {
			this.sum = sum;
		}

		@JmxAttribute
		public long getSum() {
			return sum;
		}

		@JmxAttribute
		public void setSum(long sum) {
			this.sum = sum;
		}
	}

	public static final class MBeanWithIsGetter implements ConcurrentJmxMBean {

		@JmxAttribute
		public boolean isActive() {
			return true;
		}
	}

	public static final class MBeanWithJmxAttributesOfArbitraryTypes implements ConcurrentJmxMBean {
		private final ArbitraryType arbitraryType;
		private final Date date;

		public MBeanWithJmxAttributesOfArbitraryTypes(ArbitraryType arbitraryType, Date date) {
			this.arbitraryType = arbitraryType;
			this.date = date;
		}

		@JmxAttribute
		public ArbitraryType getArbitraryType() {
			return arbitraryType;
		}

		@JmxAttribute
		public Date getDate() {
			return date;
		}
	}

	public static final class ArbitraryType {
		private final String str;

		public ArbitraryType(String str) {
			this.str = str;
		}

		@Override
		public String toString() {
			return str;
		}
	}
}
