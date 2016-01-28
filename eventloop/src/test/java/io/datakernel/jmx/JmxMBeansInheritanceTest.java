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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JmxMBeansInheritanceTest {

	@Test
	public void itShouldConsiderClassAsJmxMBeanIfOneOfItsParentClassInHierarchyIsAnnotatedWithJmxMBean()
			throws Exception {

		ConcreteMonitorable monitorable = new ConcreteMonitorable();
		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable));

		assertEquals(10, mbean.getAttribute("baseCount"));
		assertEquals(20, mbean.getAttribute("concreteCount"));
	}

	@JmxMBean
	public static abstract class AbstractMonitorable {
		@JmxAttribute
		public int getBaseCount() {
			return 10;
		}
	}

	// not annotated with @JmxMBean
	public static final class ConcreteMonitorable extends AbstractMonitorable {
		@JmxAttribute
		public int getConcreteCount() {
			return 20;
		}
	}
}
