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
import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static java.util.Arrays.asList;

public class JmxMBeansAttributeExceptionsTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void concurrentJmxMBeansAreNotAllowedToBeInPool_OnlyEventloopJmxMBeansAreAllowedToBeInPool() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("ConcurrentJmxMBeans cannot be used in pool. " +
				"Only EventloopJmxMBeans can be used in pool");

		JmxMBeans.factory().createFor(
				asList(new ConcurrentJmxMBeanWithSingleIntAttr(), new ConcurrentJmxMBeanWithSingleIntAttr()), false);
	}

	@Test
	public void jmxStatsAreAllowedOnlyInEventloopJmxMBean() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("JmxRefreshableStats can be used only in classes that implements" +
				" EventloopJmxMBean");

		JmxMBeans.factory().createFor(asList(new ConcurrentJmxMBeanWithJmxStats()), false);
	}

	// test JmxRefreshableStats as @JmxAttribute, all returned stats should be concrete classes with public no-arg constructor
	@Test
	public void jmxStatsAttributeCannotBeInterface() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class that implements" +
				" JmxRefreshableStats interface and contains public no-arg constructor");

		JmxMBeans.factory().createFor(asList(new MBeanWithInterfaceAsJmxStatsAttributes()), false);
	}

	@Test
	public void jmxStatsAttributeCannotBeAbstractClass() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class that implements" +
				" JmxRefreshableStats interface and contains public no-arg constructor");

		JmxMBeans.factory().createFor(asList(new MBeanWithAbstractClassAsJmxStatsAttributes()), false);
	}

	@Test
	public void jmxStatsAttributesClassMustHavePublicNoArgConstructor() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class that implements" +
				" JmxRefreshableStats interface and contains public no-arg constructor");

		JmxMBeans.factory().createFor(asList(new MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor()), false);
	}

	public static final class ConcurrentJmxMBeanWithJmxStats implements ConcurrentJmxMBean {

		@JmxAttribute
		public JmxStatsStub getStats() {
			return new JmxStatsStub();
		}
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

		@Override
		public Eventloop getEventloop() {
			return new Eventloop();
		}
	}

	public interface JmxStatsAdditionalInterface extends JmxRefreshableStats<JmxStatsAdditionalInterface> {
	}

	public static final class MBeanWithAbstractClassAsJmxStatsAttributes implements EventloopJmxMBean {

		@JmxAttribute
		public JmxStatsAbstractClass getStats() {
			return null;
		}

		@Override
		public Eventloop getEventloop() {
			return new Eventloop();
		}
	}

	public static abstract class JmxStatsAbstractClass implements JmxRefreshableStats<JmxStatsAbstractClass> {

	}

	public static final class MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor implements EventloopJmxMBean {

		@JmxAttribute
		public JmxStatsWithNoPublicNoArgConstructor getStats() {
			return null;
		}

		@Override
		public Eventloop getEventloop() {
			return new Eventloop();
		}
	}

	public static final class JmxStatsWithNoPublicNoArgConstructor
			implements JmxRefreshableStats<JmxStatsWithNoPublicNoArgConstructor> {
		private final int count;

		public JmxStatsWithNoPublicNoArgConstructor(int count) {
			this.count = count;
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
