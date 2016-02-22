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
import javax.management.ObjectName;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;

public class Temp {

	@Test
	// TODO(vmykhalko): add unit test for this deep recursion case (mbean -> list -> pojo -> list -> pojo)
	public void test() throws Exception {
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new TempService()), false);
//
//		ManagementFactory.getPlatformMBeanServer().registerMBean(
//				mbean, new ObjectName("io.check:type=TempService"));
//
//		Thread.sleep(Long.MAX_VALUE);
	}

	@Test
	public void test2() throws Exception {
////		boolean result = Object[].class.isAssignableFrom(String[].class);
//		boolean result = Long.TYPE.equals(long.class);
//		System.out.println(result);

//		SomeGen<String> gen = new SomeGen<>("name");
//
//		Method method = gen.getClass().getMethod("getValue");
//
//		System.out.println(method);

//
//		acc.add("String");
//
//		System.out.println("bingo");

//		Object arr = new String[]{"abc", "essqv"};
//		Class<?> clazz = String[].class;
//		System.out.println(clazz.getComponentType().getName());

//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(new TempService2(new Person("Nick", null))), false);
//
//		ManagementFactory.getPlatformMBeanServer().registerMBean(
//				mbean, new ObjectName("io.check:type=TempService2"));
//
//		Thread.sleep(Long.MAX_VALUE);
//
//		ArrayType<?> arrType = new ArrayType<>(1, new ArrayType<>(1, new ArrayType<>(1, SimpleType.INTEGER)));
//		System.out.println(arrType);

//		Method method = Something.class.getMethod("getMap");
//		Method methodT = SomethingT.class.getMethod("getMap");
//
//
//		ParameterizedType parameterizedType = (ParameterizedType) method.getGenericReturnType();
//		ParameterizedType parameterizedTypeT = (ParameterizedType) methodT.getGenericReturnType();
//
//		System.out.println(parameterizedType);
//		System.out.println(parameterizedTypeT);

		Method method = SomethingL.class.getMethod("getList");

		System.out.println(method);


	}

	@Test
	public void test3() throws NoSuchMethodException, OpenDataException {
//		Method method = Temp3.class.getMethod("getter");
//		System.out.println(method);
		ArrayType arrayType = new ArrayType(1, new ArrayType<>(1, SimpleType.STRING));
		CompositeData cd = CompositeDataBuilder.builder("bingo").add("count", SimpleType.INTEGER, 10).build();
		System.out.println(arrayType.getClassName());



//		PojoAttributeNode root = JmxMBeans.createAttributesTree(TempService.class);
//		JmxStatsStub stats_1 = new JmxStatsStub();
//		stats_1.recordValue(100);
//		JmxStatsStub stats_2 = new JmxStatsStub();
//		stats_2.recordValue(120);
//
//		Map<String, JmxStatsStub> map_1 = new HashMap<>();
//		map_1.put("key-1", stats_1);
//		map_1.put("key-2", stats_2);
//
//
//		JmxStatsStub stats_2_1 = new JmxStatsStub();
//		stats_2_1.recordValue(130);
//		JmxStatsStub stats_3 = new JmxStatsStub();
//		stats_3.recordValue(1000);
//
//		Map<String, JmxStatsStub> map_2 = new HashMap<>();
//		map_2.put("key-2", stats_2_1);
//		map_2.put("key-3", stats_3);
//
//		Object object = root.aggregateAllAttributes(asList(new TempService(map_1), new TempService(map_2)));
//		System.out.println(object);
	}

	public static final class TempService {
		private final Map<String, JmxStatsStub> map;

		public TempService(Map<String, JmxStatsStub> map) {
			this.map = map;
		}

		@JmxAttribute
		public Map<String, JmxStatsStub> getMapAttr() {
			return map;
		}

		//		@JmxAttribute
//		public List<PojoOne> getListAttr() {
//			return asList(new PojoOne("Star Wars", 10), new PojoOne("Lukas Films", 250));
//		}
//		@JmxAttribute
//		public long getCount() {
//			return count;
//		}
//
//		@JmxAttribute
//		public PojoOne getPojoOneInstance() {
//			return new PojoOne();
//		}


	}

	public static final class PojoOne {
		private final String info;
		private final int id;

		public PojoOne(String info, int id) {
			this.info = info;
			this.id = id;
		}

		@JmxAttribute
		public String getInfo() {
			return info;
		}

		@JmxAttribute
		public int getId() {
			return id;
		}
	}

	public static final class Temp3 {

		public JmxStatsStub getter() {
			return null;
		}
	}

	public static class Something {

		public Map<String, Long> getMap() {
			return null;
		}
	}

	public static class SomethingT<Q> {
		public Map<Q, Q> getMap() {
			return null;
		}
	}

	public static class SomethingL {
		public List<List<String>> getList() {
			return null;
		}
	}

	public static class SomeGen<T> {
		private T value;

		public SomeGen(T value) {
			this.value = value;
		}

		public T getValue() {
			return value;
		}
	}

	public static final class TempService2 implements ConcurrentJmxMBean {

		private final Person person;

		public TempService2(Person person) {
			this.person = person;
		}

		@JmxAttribute
		public Person getPerson() {
			return person;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static final class Person {
		private String name;
		private String surname;

		public Person(String name, String surname) {
			this.name = name;
			this.surname = surname;
		}

		@JmxAttribute
		public String getName() {
			return name;
		}

		@JmxAttribute
		public String getSurname() {
			return surname;
		}
	}

//	public interface TempServiceMXBean {
//		Map<String, Map<String, Person>> getCountryToPostIdToPerson();
//	}
//
//
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

//	public interface TempServiceMBean {
//		Object[] getArr() throws OpenDataException;
//	}
//
//	public static final class TempService implements TempServiceMBean {
//
//		@Override
//		public Object[] getArr() throws OpenDataException {
//			CompositeData data = CompositeDataBuilder.builder("qname")
//					.add("count", SimpleType.INTEGER, 10)
//					.add("desc", SimpleType.STRING, "John")
//					.build();
//			return new CompositeData[]{data};
//		}
//	}

//	public static final class TempService implements ConcurrentJmxMBean {
//		private List<Person> persons =
//				asList(new Person(10, "Winston", asList(new Address("5th avenu", 15), new Address("Ford", 23))),
//						new Person(150, "Alberto", new ArrayList<Address>()),
//						new Person(305, "Lukas", asList(new Address("Google", 205))));
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
//
//	public static final class Person {
//		private int id;
//		private String name;
//		private List<Address> addresses;
//
//		public Person(int id, String name, List<Address> addresses) {
//			this.id = id;
//			this.name = name;
//			this.addresses = addresses;
//		}
//
//		@JmxAttribute
//		public int getId() {
//			return id;
//		}
//
//		@JmxAttribute
//		public String getName() {
//			return name;
//		}
//
//		@JmxAttribute
//		public List<Address> getAddresses() {
//			return addresses;
//		}
//	}
//
//	public static final class Address {
//		private String street;
//		private int number;
//
//		public Address(String street, int number) {
//			this.street = street;
//			this.number = number;
//		}
//
//		@JmxAttribute
//		public String getStreet() {
//			return street;
//		}
//
//		@JmxAttribute
//		public int getNumber() {
//			return number;
//		}
//	}
}
