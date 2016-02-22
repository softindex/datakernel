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
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;

public class Temp3 {

	@Test
	public void test() throws Exception {
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new TempService("bingo")), true);
		ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, new ObjectName("io.check:type=TempService"));

		System.out.println("waiting...");
		Thread.sleep(Long.MAX_VALUE);
	}

	public static final class TempService implements ConcurrentJmxMBean {
//		private long sum;
//		private int count;

		private final PojoOne pojoOne;

		public TempService(String name) {
			this.pojoOne = new PojoOne(name);
		}

		@JmxAttribute
		public PojoOne getPojoOne() {
			return pojoOne;
		}


		//		@JmxAttribute
//		public int getCount() {
//			return count;
//		}
//
//		@JmxAttribute
//		public long getSum() {
//			return sum;
//		}
//
//		@JmxOperation
//		public void add(long number) {
//			sum += number;
//			count++;
//		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static final class PojoOne {
		private final String info;
		private StubStats stats;
		private final Map<String, StubStats> map;

		public PojoOne(String info) {
			this.info = info;
			this.stats = new StubStats(info);
			this.map = new HashMap<>();
			map.put("key-1", new StubStats("stats_1"));
			map.put("key-2", new StubStats("stats_2"));
		}

		@JmxAttribute
		public Map<String, StubStats> getMap() {
			return map;
		}

		@JmxAttribute
		public String getInfo() {
			return info;
		}

		@JmxAttribute
		public StubStats getStats() {
			return stats;
		}
	}

	public static final class StubStats implements JmxStats<StubStats> {
		private final String name;
		private int updated = 0;

		public StubStats() {
			this.name = "accumulator";
		}

		public StubStats(String name) {
			this.name = name;
		}

		@JmxAttribute
		public int getUpdated() {
			return updated;
		}

		@JmxAttribute
		public String getName() {
			return name;
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			System.out.println(name + " refreshed " + timestamp + " " + smoothingWindow);
			updated++;
			System.out.println("updated times total: " + updated);
		}

		@Override
		public void add(StubStats value) {
			this.updated += value.updated;
		}
	}
}
