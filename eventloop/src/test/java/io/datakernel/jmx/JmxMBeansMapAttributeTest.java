///*
// * Copyright (C) 2015 SoftIndex LLC.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.datakernel.jmx;
//
//import io.datakernel.jmx.helper.JmxStatsStub;
//import org.junit.Test;
//
//import javax.management.DynamicMBean;
//import javax.management.MBeanInfo;
//import javax.management.openmbean.CompositeData;
//import javax.management.openmbean.TabularData;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.Executor;
//import java.util.concurrent.Executors;
//
//import static java.util.Arrays.asList;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//
//public class JmxMBeansMapAttributeTest {
//
//	@Test
//	public void itShouldProperlyCollectAndReturnMapAttribute() throws Exception {
//		Map<String, JmxStatsStub> map = new HashMap<>();
//
//		map.put("key-1", new JmxStatsStub());
//		map.put("key-2", new JmxStatsStub());
//
//		map.get("key-1").recordValue(52L);
//		map.get("key-1").recordValue(23L);
//		map.get("key-2").recordValue(187L);
//
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new MBeanWithMapOfStats(map)), false);
//
//		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
//		assertEquals(1, mBeanInfo.getAttributes().length);
//
//		TabularData tabularData = (TabularData) mbean.getAttribute("mapAttr");
//
//		CompositeData rowWithKey_1 = tabularData.get(new Object[]{"key-1"});
//		assertEquals(52L + 23L, (long) rowWithKey_1.get("sum"));
//		assertEquals(2, (int) rowWithKey_1.get("count"));
//
//		CompositeData rowWithKey_2 = tabularData.get(new Object[]{"key-2"});
//		assertEquals(187L, (long) rowWithKey_2.get("sum"));
//		assertEquals(1, (int) rowWithKey_2.get("count"));
//	}
//
//	@Test
//	public void itShouldProperlyAggregateMapWithJmxStats() throws Exception {
//		// init
//		Map<String, JmxStatsStub> map_1 = new HashMap<>();
//		map_1.put("key-1", new JmxStatsStub());
//		map_1.put("key-2", new JmxStatsStub());
//		map_1.get("key-1").recordValue(52L);
//		map_1.get("key-2").recordValue(187L);
//
//		Map<String, JmxStatsStub> map_2 = new HashMap<>();
//		map_2.put("key-2", new JmxStatsStub());
//		map_2.put("key-3", new JmxStatsStub());
//		map_2.get("key-2").recordValue(33L);
//		map_2.get("key-2").recordValue(15L);
//		map_2.get("key-3").recordValue(71L);
//
//		DynamicMBean mbean = JmxMBeans.factory().createFor(
//				asList(
//						new MBeanWithMapOfStats(map_1),
//						new MBeanWithMapOfStats(map_2)
//				),
//				false);
//
//		// check
//		TabularData tabularData = (TabularData) mbean.getAttribute("mapAttr");
//
//		CompositeData rowWithKey_1 = tabularData.get(new Object[]{"key-1"});
//		assertEquals(52L, (long) rowWithKey_1.get("sum"));
//		assertEquals(1, (int) rowWithKey_1.get("count"));
//
//		CompositeData rowWithKey_2 = tabularData.get(new Object[]{"key-2"});
//		assertEquals(187L + 33L + 15L, (long) rowWithKey_2.get("sum"));
//		assertEquals(3, (int) rowWithKey_2.get("count"));
//
//		CompositeData rowWithKey_3 = tabularData.get(new Object[]{"key-3"});
//		assertEquals(71L, (long) rowWithKey_3.get("sum"));
//		assertEquals(1, (int) rowWithKey_3.get("count"));
//
//		// check for side-effects absence
//		assertEquals(52L, map_1.get("key-1").getSum());
//		assertEquals(187L, map_1.get("key-2").getSum());
//
//		assertEquals(33L + 15L, map_2.get("key-2").getSum());
//		assertEquals(71L, map_2.get("key-3").getSum());
//	}
//
//	@Test
//	public void itShouldProperlyAggregateMapWithPojos() throws Exception {
//		// init
//		Map<String, PojoStats> map_1 = new HashMap<>();
//		map_1.put("key-1", new PojoStats(1, "name-1"));
//		map_1.put("key-2", new PojoStats(2, "name-2"));
//
//		Map<String, PojoStats> map_2 = new HashMap<>();
//		map_2.put("key-2", new PojoStats(15, "name-2"));
//		map_2.put("key-3", new PojoStats(3, "name-3"));
//
//		DynamicMBean mbean = JmxMBeans.factory().createFor(
//				asList(
//						new MBeanWithMapOfPojos(map_1),
//						new MBeanWithMapOfPojos(map_2)
//				),
//				false);
//
//		// check
//		TabularData tabularData = (TabularData) mbean.getAttribute("mapAttr");
//
//		CompositeData rowWithKey_1 = tabularData.get(new Object[]{"key-1"});
//		assertEquals(1, (int) rowWithKey_1.get("count"));
//		assertEquals("name-1", rowWithKey_1.get("name"));
//
//		CompositeData rowWithKey_2 = tabularData.get(new Object[]{"key-2"});
//		// in map entry with key "key-2" attributes with name "count" have different values, so we cannot aggregate them
//		assertNull(rowWithKey_2.get("count"));
//		assertEquals("name-2", rowWithKey_2.get("name"));
//
//		CompositeData rowWithKey_3 = tabularData.get(new Object[]{"key-2"});
//		assertEquals(3, (int) rowWithKey_3.get("count"));
//		assertEquals("name-3", rowWithKey_3.get("name"));
//	}
//
//	@Test
//	public void itShouldProperlyHandleCustomKeys() throws Exception {
//		Map<CustomKey, Long> map = new HashMap<>();
//
//		map.put(new CustomKey(10), 100500L);
//		map.put(new CustomKey(20), 200500L);
//
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new MBeanWithMapOfCustomKey(map)), false);
//
//		TabularData tabularData = (TabularData) mbean.getAttribute("mapAttr");
//
//		// keys of CompositeData should be formed using CustomKey.toString()
//		CompositeData rowWithKey_1 = tabularData.get(new Object[]{"id=10"});
//		assertEquals(100500L, (long) rowWithKey_1.get("value"));
//
//		CompositeData rowWithKey_2 = tabularData.get(new Object[]{"id=20"});
//		assertEquals(200500L, (long) rowWithKey_2.get("value"));
//	}
//
//	public static final class MBeanWithMapOfStats implements ConcurrentJmxMBean {
//		private final Map<String, JmxStatsStub> mapAttr;
//
//		public MBeanWithMapOfStats(Map<String, JmxStatsStub> mapAttr) {
//			this.mapAttr = mapAttr;
//		}
//
//		@JmxAttribute
//		public Map<String, JmxStatsStub> getMapAttr() {
//			return mapAttr;
//		}
//
//		@Override
//		public Executor getJmxExecutor() {
//			return Executors.newSingleThreadExecutor();
//		}
//	}
//
//	public static final class MBeanWithMapOfPojos implements ConcurrentJmxMBean {
//		private final Map<String, PojoStats> mapAttr;
//
//		public MBeanWithMapOfPojos(Map<String, PojoStats> mapAttr) {
//			this.mapAttr = mapAttr;
//		}
//
//		@JmxAttribute
//		public Map<String, PojoStats> getMapAttr() {
//			return mapAttr;
//		}
//
//		@Override
//		public Executor getJmxExecutor() {
//			return Executors.newSingleThreadExecutor();
//		}
//	}
//
//	public static final class PojoStats {
//		private final int count;
//		private final String name;
//
//		public PojoStats(int count, String name) {
//			this.count = count;
//			this.name = name;
//		}
//
//		@JmxAttribute
//		public int getCount() {
//			return count;
//		}
//
//		@JmxAttribute
//		public String getName() {
//			return name;
//		}
//	}
//
//	public static final class MBeanWithMapOfCustomKey implements ConcurrentJmxMBean {
//		private final Map<CustomKey, Long> mapAttr;
//
//		public MBeanWithMapOfCustomKey(Map<CustomKey, Long> mapAttr) {
//			this.mapAttr = mapAttr;
//		}
//
//		@JmxAttribute
//		public Map<CustomKey, Long> getMapAttr() {
//			return mapAttr;
//		}
//
//		@Override
//		public Executor getJmxExecutor() {
//			return Executors.newSingleThreadExecutor();
//		}
//	}
//
//	public static final class CustomKey {
//		private final int id;
//
//		public CustomKey(int id) {
//			this.id = id;
//		}
//
//		@Override
//		public String toString() {
//			return "id=" + id;
//		}
//	}
//}
