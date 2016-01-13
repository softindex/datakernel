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
import io.datakernel.jmx.annotation.JmxMBean;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class JmxRegistryTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private DynamicMBeanFactory mbeanFactory = context.mock(DynamicMBeanFactory.class);
	private DynamicMBean dynamicMBean = context.mock(DynamicMBean.class);
	private JmxRegistry jmxRegistry = new JmxRegistry(mBeanServer, mbeanFactory);
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void registerSingletonInstanceWithout_Annotation_AndComposeAppropriateObjectName() throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(service);
			will(returnValue(dynamicMBean));

			exactly(1).of(mBeanServer).registerMBean(with(dynamicMBean), with(objectname(domain + ":type=ServiceStub")));
		}});

		Key<?> key_1 = Key.get(ServiceStub.class);
		jmxRegistry.registerInstance(key_1, service);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithoutParameters_AndComposeAppropriateObjectName() throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(service);
			will(returnValue(dynamicMBean));

			exactly(1).of(mBeanServer).registerMBean(with(dynamicMBean), with(objectname(domain + ":type=BasicService")));
		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.get(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.registerInstance(key, service);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithOneDefaultParameter_AndComposeAppropriateObjectName()
			throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(service);
			will(returnValue(dynamicMBean));

			exactly(1).of(mBeanServer).registerMBean(with(dynamicMBean), with(objectname(domain + ":Group=major")));
		}});

		Group groupAnnotation = createGroupAnnotation("major");
		Key<?> key = Key.get(ServiceStub.class, groupAnnotation);
		jmxRegistry.registerInstance(key, service);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithSeveralParameters_AndComposeAppropriateObjectName()
			throws Exception {
		final ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory).createFor(service);
			will(returnValue(dynamicMBean));

			exactly(1).of(mBeanServer).registerMBean(
					with(dynamicMBean), with(objectname(domain + ":threadId=1,threadName=thread-one")));
		}});

		ComplexAnnotation complexAnnotation = createComplexAnnotation(1, "thread-one");
		Key<?> key = Key.get(ServiceStub.class, complexAnnotation);
		jmxRegistry.registerInstance(key, service);

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
	@JmxMBean
	public final class ServiceStub {

	}

	// custom matcher
	public static Matcher<ObjectName> objectname(final String name) {
		return new BaseMatcher<ObjectName>() {
			@Override
			public boolean matches(Object item) {
				try {
					return new ObjectName(name).equals((ObjectName) item);
				} catch (MalformedObjectNameException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("ObjectName: " + name);
			}
		};
	}

}
