/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
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
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.management.DynamicMBean;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.jmx.MBeanSettings.defaultSettings;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;

public class JmxMBeansAttributesArraysTest {
	private static final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

	@Test
	public void properlyGetsArrayOfInts() throws Exception {
		MBeanWithIntArray mBeanWithIntArray = new MBeanWithIntArray();
		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithIntArray);

		Integer[] expected = {1, 2, 3};
		assertArrayEquals(expected, (Integer[]) mbean.getAttribute("integerNumbers"));
	}

	@Test
	public void properlyGetsArrayOfDoubles() throws Exception {
		MBeanWithDoubleArray mBeanWithDoubleArray = new MBeanWithDoubleArray();
		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithDoubleArray);

		Double[] expected = {1.1, 2.2, 3.3};
		assertArrayEquals(expected, (Double[]) mbean.getAttribute("doubleNumbers"));
	}

	@Test
	public void concatsArraysWhileAggregatingSeveralMBeans() throws Exception {
		MBeanWithIntArray mBeanWithIntArray1 = new MBeanWithIntArray();
		MBeanWithIntArray mBeanWithIntArray2 = new MBeanWithIntArray();

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithIntArray1, mBeanWithIntArray2);

		Integer[] expected = {1, 2, 3, 1, 2, 3};
		assertArrayEquals(expected, (Integer[]) mbean.getAttribute("integerNumbers"));
	}

	// region helper classes
	public static class MBeanWithIntArray implements EventloopJmxMBean {

		@JmxAttribute
		public int[] getIntegerNumbers() {
			return new int[]{1, 2, 3};
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return eventloop;
		}
	}

	public static class MBeanWithDoubleArray implements ConcurrentJmxMBean {

		@JmxAttribute
		public double[] getDoubleNumbers() {
			return new double[]{1.1, 2.2, 3.3};
		}
	}
	// endregion

	// region helper methods
	public static DynamicMBean createDynamicMBeanFor(Object... objects) {
		return JmxMBeans.factory().createFor(asList(objects), defaultSettings(), false);
	}
	// endregion
}
