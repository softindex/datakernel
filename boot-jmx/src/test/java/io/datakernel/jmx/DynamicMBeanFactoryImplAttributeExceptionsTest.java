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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBean;
import io.datakernel.eventloop.jmx.JmxRefreshableStats;
import io.datakernel.jmx.api.ConcurrentJmxMBean;
import io.datakernel.jmx.api.JmxAttribute;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.jmx.MBeanSettings.defaultSettings;
import static java.util.Arrays.asList;

public class DynamicMBeanFactoryImplAttributeExceptionsTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void concurrentJmxMBeansAreNotAllowedToBeInPool_OnlyEventloopJmxMBeansAreAllowedToBeInPool() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("ConcurrentJmxMBeans cannot be used in pool. " +
				"Only EventloopJmxMBeans can be used in pool");

		DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(
						asList(new ConcurrentJmxMBeanWithSingleIntAttr(), new ConcurrentJmxMBeanWithSingleIntAttr()),
						defaultSettings(), false);
	}

	// test JmxRefreshableStats as @JmxAttribute, all returned stats should be concrete classes with public no-arg constructor
	@Test
	public void jmxStatsAttributeCannotBeInterface() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor");

		DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(asList(new MBeanWithInterfaceAsJmxStatsAttributes()), defaultSettings(), false);
	}

	@Test
	public void jmxStatsAttributeCannotBeAbstractClass() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor");

		DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(asList(new MBeanWithAbstractClassAsJmxStatsAttributes()), defaultSettings(), false);
	}

	@Test
	public void jmxStatsAttributesClassMustHavePublicNoArgConstructor() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor");

		DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(asList(new MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor()), defaultSettings(), false);
	}

	public static final class ConcurrentJmxMBeanWithSingleIntAttr implements ConcurrentJmxMBean {

		@JmxAttribute
		public int getCount() {
			return 0;
		}
	}

	public static final class MBeanWithInterfaceAsJmxStatsAttributes implements EventloopJmxMBean {

		@JmxAttribute
		public JmxStatsAdditionalInterface getStats() {
			return null;
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		}
	}

	public interface JmxStatsAdditionalInterface extends JmxRefreshableStats<JmxStatsAdditionalInterface> {
	}

	public static final class MBeanWithAbstractClassAsJmxStatsAttributes implements EventloopJmxMBean {

		@JmxAttribute
		public JmxStatsAbstractClass getStats() {
			return null;
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		}
	}

	public static abstract class JmxStatsAbstractClass implements JmxRefreshableStats<JmxStatsAbstractClass> {

	}

	public static final class MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor implements EventloopJmxMBean {

		@JmxAttribute
		public JmxStatsWithNoPublicNoArgConstructor getStats() {
			return null;
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		}
	}

	public static final class JmxStatsWithNoPublicNoArgConstructor
			implements JmxRefreshableStats<JmxStatsWithNoPublicNoArgConstructor> {
		public JmxStatsWithNoPublicNoArgConstructor(int count) {
		}

		@JmxAttribute
		public int getAttr() {
			return 0;
		}

		@Override
		public void add(JmxStatsWithNoPublicNoArgConstructor another) {

		}

		@Override
		public void refresh(long timestamp) {

		}
	}
}
