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
import java.lang.management.ManagementFactory;

import static java.lang.String.format;

public class Temp {

	@Test
	public void test() throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException, InterruptedException {
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

	public interface TempServiceMXBean {
		String getName() throws MBeanException;
	}

	public static final class TempService implements TempServiceMXBean {

		@Override
		public String getName() throws MBeanException {
			Exception attrException = new IllegalArgumentException("bingo-wales");
			throw new MBeanException(new Exception(
					format("Exception with type \"%s\" and message \"%s\" occured during fetching attribute",
							attrException.getClass().getName(), attrException.getMessage())));
		}
	}

	public static final class Person {
		private int id;
		private String name;

		public Person(int id, String name) {
			this.id = id;
			this.name = name;
		}

		public int getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}
}
