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

import javax.management.*;

import static io.datakernel.jmx.helper.CustomMatchers.objectname;

public class MXBeansRegistrationTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private DynamicMBeanFactory mbeanFactory = context.mock(DynamicMBeanFactory.class);
	private JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void itShouldRegisterStandardMBeans() throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service), with(objectname(domain + ":type=ServiceStub")));
		}});

		Key<?> key = Key.get(ServiceStub.class);
		jmxRegistry.registerSingleton(key, service, null);
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

	// region transitive implementations of MXBean interface through another interface
	@Test
	public void itShouldRegisterClassWhichImplementsMXBeanInterfaceTransivelyThroughAnotherInterface()
			throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
		final ServiceTransitiveInterface service = new ServiceTransitiveInterface();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(
					with(service), with(objectname(domain + ":type=ServiceTransitiveInterface"))
			);
		}});

		Key<?> key = Key.get(ServiceTransitiveInterface.class);
		jmxRegistry.registerSingleton(key, service, null);
	}

	public interface InterfaceMXBean {
		int getCount();
	}

	public interface TransitiveInterface extends InterfaceMXBean {

	}

	public class ServiceTransitiveInterface implements TransitiveInterface {

		@Override
		public int getCount() {
			return 0;
		}
	}
	// endregion

	// region transitive implementation of MXBean interface through abstract class
	@Test
	public void itShouldRegisterClassWhichImplementsMXBeanInterfaceTransivelyThroughAbstractClass()
			throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
		final ServiceTransitiveClass service = new ServiceTransitiveClass();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(
					with(service), with(objectname(domain + ":type=ServiceTransitiveClass"))
			);
		}});

		Key<?> key = Key.get(ServiceTransitiveClass.class);
		jmxRegistry.registerSingleton(key, service, null);
	}

	public abstract class TransitiveClass implements InterfaceMXBean {

	}

	public class ServiceTransitiveClass extends TransitiveClass {

		@Override
		public int getCount() {
			return 0;
		}
	}
	// endregion

	// region registration of class that implements interface with @MXBean annotation
	@Test
	public void itShouldRegisterClassWhichImplementsInterfaceAnnotatedWithMXBean()
			throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
		final ServiceWithMXBeanInterfaceAnnotation service = new ServiceWithMXBeanInterfaceAnnotation();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(
					with(service), with(objectname(domain + ":type=ServiceWithMXBeanInterfaceAnnotation"))
			);
		}});

		Key<?> key = Key.get(ServiceWithMXBeanInterfaceAnnotation.class);
		jmxRegistry.registerSingleton(key, service, null);
	}

	@MXBean
	public interface JMXInterfaceWithAnnotation {
		int getCount();
	}

	public class ServiceWithMXBeanInterfaceAnnotation implements JMXInterfaceWithAnnotation {
		@Override
		public int getCount() {
			return 0;
		}
	}
	// endregion
}
