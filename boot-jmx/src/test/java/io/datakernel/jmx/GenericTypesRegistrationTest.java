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

import io.datakernel.di.Key;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.MBeanServer;
import java.util.List;
import java.util.Map;

import static io.datakernel.jmx.MBeanSettings.defaultSettings;
import static io.datakernel.jmx.helper.CustomMatchers.objectname;

public class GenericTypesRegistrationTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, DynamicMBeanFactoryImpl.create());
	private final String domain = ServiceStubOne.class.getPackage().getName();

	@Test
	public void itShouldFormProperNameForTypeWithSingleGenericParameter() throws Exception {
		ServiceStubOne<?> service = new ServiceStubOne();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service),
					with(objectname(domain + ":type=ServiceStubOne,T1=String")));
		}});

		Key<?> key = new Key<ServiceStubOne<String>>() {};
		jmxRegistry.registerSingleton(key, service, defaultSettings());
	}

	@Test
	public void itShouldFormProperNameForTypeWithSeveralGenericParameter() throws Exception {
		ServiceStubThree<String, Integer, Long> service = new ServiceStubThree<>();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service),
					with(objectname(domain + ":type=ServiceStubThree,T1=String,T2=Integer,T3=Long"))
			);
		}});

		Key<?> key = new Key<ServiceStubThree<String, Integer, Long>>() {};
		jmxRegistry.registerSingleton(key, service, defaultSettings());
	}

	@Test
	public void itShouldFormProperNameForTypeWithNestedGenerics() throws Exception {
		ServiceStubThree<String, List<Integer>, Map<Long, List<String>>> service = new ServiceStubThree<>();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service),
					with(objectname(domain + ":type=ServiceStubThree,T1=String,T2=List<Integer>,T3=Map<Long;List<String>>"))
			);
		}});

		Key<?> key = new Key<ServiceStubThree<String, List<Integer>, Map<Long, List<String>>>>() {};
		jmxRegistry.registerSingleton(key, service, defaultSettings());
	}

	public interface ServiceStubOneMBean {
		int getCount();
	}

	public static class ServiceStubOne<T> implements ServiceStubOneMBean {
		private T value;

		@Override
		public int getCount() {
			return 0;
		}
	}

	public interface ServiceStubThreeMBean {
		int getCount();
	}

	public static class ServiceStubThree<A, B, C> implements ServiceStubThreeMBean {
		private A a;
		private B b;
		private C c;

		@Override
		public int getCount() {
			return 0;
		}
	}
}
