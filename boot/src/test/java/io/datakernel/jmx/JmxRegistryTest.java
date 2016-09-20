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

import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import static io.datakernel.jmx.helper.CustomMatchers.objectname;
import static java.util.Arrays.asList;

public class JmxRegistryTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private DynamicMBeanFactory mbeanFactory = context.mock(DynamicMBeanFactory.class);
	private DynamicMBean dynamicMBean = context.mock(DynamicMBean.class);
	private JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void registerSingletonInstanceWithout_Annotation_AndComposeAppropriateObjectName() throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(asList(service), true);
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(with(dynamicMBean), with(objectname(domain + ":type=ServiceStub")));
		}});

		Key<?> key_1 = Key.get(ServiceStub.class);
		jmxRegistry.registerSingleton(key_1, service);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithoutParameters_AndComposeAppropriateObjectName()
			throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(asList(service), true);
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService")));
		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.get(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.registerSingleton(key, service);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithOneDefaultParameter_AndComposeAppropriateObjectName()
			throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(asList(service), true);
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(with(dynamicMBean), with(objectname(domain + ":Group=major")));
		}});

		Group groupAnnotation = createGroupAnnotation("major");
		Key<?> key = Key.get(ServiceStub.class, groupAnnotation);
		jmxRegistry.registerSingleton(key, service);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithSeveralParameters_AndComposeAppropriateObjectName()
			throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(asList(service), true);
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean), with(objectname(domain + ":threadId=1,threadName=thread-one")));
		}});

		ComplexAnnotation complexAnnotation = createComplexAnnotation(1, "thread-one");
		Key<?> key = Key.get(ServiceStub.class, complexAnnotation);
		jmxRegistry.registerSingleton(key, service);

	}

	@Test
	public void unregisterSingletonInstance() throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).unregisterMBean(with(objectname(domain + ":type=ServiceStub,annotation=BasicService")));
		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.get(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterSingleton(key, service);
	}

	@Test
	public void registerWorkers_andComposeAppropriateObjectNames() throws Exception {
		final ServiceStub worker_1 = new ServiceStub();
		final ServiceStub worker_2 = new ServiceStub();
		final ServiceStub worker_3 = new ServiceStub();

		context.checking(new Expectations() {{
			// creating DynamicMBeans for each worker separately
			allowing(mbeanFactory).createFor(asList(worker_1), false);
			will(returnValue(dynamicMBean));

			allowing(mbeanFactory).createFor(asList(worker_2), false);
			will(returnValue(dynamicMBean));

			allowing(mbeanFactory).createFor(asList(worker_3), false);
			will(returnValue(dynamicMBean));

			// creating DynamicMBean for worker pool
			allowing(mbeanFactory).createFor(asList(worker_1, worker_2, worker_3), true);
			will(doAll());
			will(returnValue(dynamicMBean));

			// checking calls and names for each worker separately
			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,workerId=worker-0")));
			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,workerId=worker-1")));
			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,workerId=worker-2")));

			// checking calls and names for worker_pool DynamicMBean
			oneOf(mBeanServer).registerMBean(with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService")));

		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.get(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.registerWorkers(key, asList(worker_1, worker_2, worker_3));
	}

	@Test
	public void unregisterWorkers() throws Exception {
		final ServiceStub worker_1 = new ServiceStub();
		final ServiceStub worker_2 = new ServiceStub();
		final ServiceStub worker_3 = new ServiceStub();

		context.checking(new Expectations() {{
			// checking calls and names for each worker separately
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,workerId=worker-0")));
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,workerId=worker-1")));
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,workerId=worker-2")));

			// checking calls and names for worker_pool DynamicMBean
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService")));

		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.get(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterWorkers(key, asList(worker_1, worker_2, worker_3));
	}

	// annotations
	@Retention(RetentionPolicy.RUNTIME)
	@BindingAnnotation
	public @interface BasicService {
	}

	@Retention(RetentionPolicy.RUNTIME)
	@BindingAnnotation
	public @interface Group {
		String value() default "";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@BindingAnnotation
	public @interface ComplexAnnotation {
		int threadId() default -1;

		String threadName() default "defaultThread";
	}

	// helpers for creating annotations
	public static BasicService createBasicServiceAnnotation() {
		return new BasicService() {
			@Override
			public Class<? extends Annotation> annotationType() {
				return BasicService.class;
			}
		};
	}

	public static Group createGroupAnnotation(final String value) {
		return new Group() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return Group.class;
			}

			@Override
			public String value() {
				return value;
			}
		};
	}

	public static ComplexAnnotation createComplexAnnotation(final int threadId, final String threadName) {
		return new ComplexAnnotation() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return ComplexAnnotation.class;
			}

			@Override
			public int threadId() {
				return threadId;
			}

			@Override
			public String threadName() {
				return threadName;
			}
		};
	}

	// helper classes
	public final class ServiceStub implements ConcurrentJmxMBean {

	}
}
