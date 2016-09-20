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

import com.google.inject.Key;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;

import static io.datakernel.jmx.helper.CustomMatchers.objectname;

public class MXBeansRegistrationTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private DynamicMBeanFactory mbeanFactory = context.mock(DynamicMBeanFactory.class);
	private DynamicMBean dynamicMBean = context.mock(DynamicMBean.class);
	private JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void itShouldRegisterStandardMBeans() throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service), with(objectname(domain + ":type=ServiceStub")));
		}});

		Key<?> key = Key.get(ServiceStub.class);
		jmxRegistry.registerSingleton(key, service);
	}

	public interface ServiceStubMXBean {
		int getCount();
	}

	public static class ServiceStub implements ServiceStubMXBean {

		@Override
		public int getCount() {
			return 0;
		}
	}
}
