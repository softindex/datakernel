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
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;

public class Temp2 {

	@Test
	public void test() throws Exception {
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new TempService()), false);

		ManagementFactory.getPlatformMBeanServer().registerMBean(
				new TempService(), new ObjectName("io.check:type=TempService"));

		Thread.sleep(Long.MAX_VALUE);
	}

	@Test
	public void test2() {
//		boolean result = Object[].class.isAssignableFrom(String[].class);
		boolean result = Long.TYPE.equals(long.class);
		System.out.println(result);
	}

//	public interface TempServiceMXBean {
//		Map<String, Map<String, Person>> getCountryToPostIdToPerson();
//	}


//	public static final class TempService implements TempServiceMXBean {
//
//		@Override
//		public Map<String, Map<String, Person>> getCountryToPostIdToPerson() {
//			Map<String, Person> map = new HashMap<>();
//			map.put("NY-1", new Person(1, "Nick"));
//			map.put("LA-5", new Person(2, "Lukas"));
//
//			List<String> names = new ArrayList<>();
//			names.toArray(new String[names.size()]);
//
//
//
//			Map<String, Map<String, Person>> mapl2 = new HashMap<>();
//			mapl2.put("USA", map);
//			return mapl2;
//		}
//	}

	public interface TempServiceMBean {
		Object[] getArr() throws OpenDataException;
	}

	public static final class TempService implements TempServiceMBean {

		@Override
		public Object[] getArr() throws OpenDataException {
			CompositeData data = CompositeDataBuilder.builder("qname")
					.add("count", SimpleType.INTEGER, 10)
					.add("desc", SimpleType.STRING, "John")
					.build();
			CompositeData data2 = CompositeDataBuilder.builder("qname")
					.add("arr", new ArrayType<>(1, data.getCompositeType()), new Object[0])
//					.add("desc", SimpleType.STRING, "Mark")
					.build();
			return new CompositeData[]{data2};
		}
	}

//	public static final class TempService implements ConcurrentJmxMBean {
//		private List<Person> persons =
//				asList(new Person(10, "Winston", asList("football", "bowling")),
//						new Person(150, "Alberto", asList("computer games")),
//						new Person(305, "Lukas", asList("tourism", "sport")));
//		private List<Long> sequence = asList(15L, 25L, 85L, 125L, 500L);
//
//		@JmxAttribute
//		public List<Person> getPersons() {
//			return persons;
//		}
//
//		@JmxAttribute
//		public List<Long> getSequence() {
//			return sequence;
//		}
//
//		@Override
//		public Executor getJmxExecutor() {
//			return Executors.newSingleThreadExecutor();
//		}
//	}

	public static final class Person {
		private int id;
		private String name;
		private List<String> hobbies;

		public Person(int id, String name, List<String> hobbies) {
			this.id = id;
			this.name = name;
			this.hobbies = hobbies;
		}

		@JmxAttribute
		public int getId() {
			return id;
		}

		@JmxAttribute
		public String getName() {
			return name;
		}

		@JmxAttribute
		public List<String> getHobbies() {
			return hobbies;
		}
	}


}
