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
import io.datakernel.di.annotation.QualifierAnnotation;
import io.datakernel.jmx.api.ConcurrentJmxBean;
import io.datakernel.jmx.api.attribute.JmxAttribute;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import static io.datakernel.jmx.JmxBeanSettings.defaultSettings;
import static io.datakernel.jmx.helper.CustomMatchers.objectname;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public class DynamicMBeanRegistrationTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private DynamicMBeanFactory mbeanFactory = DynamicMBeanFactory.create();
	private JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = CustomKeyClass.class.getPackage().getName();

	@Test
	public void itShouldRegisterDynamicMBeans() throws Exception {
		DynamicMBeanStub service = new DynamicMBeanStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service),
					with(objectname(domain + ":type=CustomKeyClass,CustomAnnotation=Global"))
			);
		}});

		Key<?> key = Key.of(CustomKeyClass.class, createCustomAnnotation("Global"));
		jmxRegistry.registerSingleton(key, service, defaultSettings());
	}

	@Test
	public void itShouldRegisterDynamicMBeansWithOverriddenAttributes() throws Exception {
		PublicMBeanSubclass instance = new PublicMBeanSubclass();
		DynamicMBean mBean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(instance), defaultSettings(), false);

		assertEquals(instance.getValue(), mBean.getAttribute("value"));
	}

	@Test
	public void itShouldThrowExceptionForNonPublicMBeans() throws Exception {
		NonPublicMBean instance = new NonPublicMBean();

		try {
			DynamicMBeanFactory.create()
					.createDynamicMBean(singletonList(instance), defaultSettings(), false);
			fail();
		} catch (IllegalStateException e) {
			assertThat(e.getMessage(), containsString("A class '" + NonPublicMBean.class.getName() +
					"' containing methods annotated with @JmxAttribute should be declared public"));
		}
	}

	@Test
	public void itShouldNotThrowExceptionForNonPublicMBeansWithNoJmxFields() throws Exception {
		NonPublicMBeanSubclass instance = new NonPublicMBeanSubclass();
		DynamicMBean mBean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(instance), defaultSettings(), false);

		assertEquals(instance.getValue(), mBean.getAttribute("value"));
	}

	// region helper classes
	public static class DynamicMBeanStub implements DynamicMBean {

		@Override
		public Object getAttribute(String attribute) {
			return null;
		}

		@Override
		public void setAttribute(Attribute attribute) {

		}

		@Override
		public AttributeList getAttributes(String[] attributes) {
			return null;
		}

		@Override
		public AttributeList setAttributes(AttributeList attributes) {
			return null;
		}

		@Override
		public Object invoke(String actionName, Object[] params, String[] signature) {
			return null;
		}

		@Override
		public MBeanInfo getMBeanInfo() {
			return null;
		}
	}

	static class NonPublicMBean implements ConcurrentJmxBean {
		@JmxAttribute
		public int getValue() {
			return 123;
		}
	}

	public static class PublicMBean implements ConcurrentJmxBean {
		@JmxAttribute
		public int getValue() {
			return 123;
		}
	}

	static class NonPublicMBeanSubclass extends PublicMBean {
	}

	public static class PublicMBeanSubclass extends PublicMBean{
		@JmxAttribute
		public int getValue() {
			return 321;
		}
	}

	public static class CustomKeyClass {

	}

	@Retention(RetentionPolicy.RUNTIME)
	@QualifierAnnotation
	public @interface CustomAnnotation {
		String value() default "";
	}

	public static CustomAnnotation createCustomAnnotation(String value) {
		return new CustomAnnotation() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return CustomAnnotation.class;
			}

			@Override
			public String value() {
				return value;
			}
		};
	}
	// endregion
}
