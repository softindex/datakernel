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

import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.core.Key;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import static io.datakernel.jmx.MBeanSettings.defaultSettings;
import static io.datakernel.jmx.helper.CustomMatchers.objectname;

public class DynamicMBeanRegistrationTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private DynamicMBeanFactory mbeanFactory = context.mock(DynamicMBeanFactory.class);
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

	public static class CustomKeyClass {

	}

	@Retention(RetentionPolicy.RUNTIME)
	@NameAnnotation
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
