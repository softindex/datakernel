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

import io.datakernel.jmx.api.ConcurrentJmxBean;
import io.datakernel.jmx.api.JmxBean;
import io.datakernel.jmx.api.attribute.JmxAttribute;
import io.datakernel.jmx.helper.JmxBeanAdapterStub;
import org.junit.Test;

import javax.management.DynamicMBean;

import static io.datakernel.jmx.JmxBeanSettings.defaultSettings;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;

public class DynamicMBeanFactoryAttributesArraysTest {

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
	@JmxBean(JmxBeanAdapterStub.class)
	public static class MBeanWithIntArray {

		@JmxAttribute
		public int[] getIntegerNumbers() {
			return new int[]{1, 2, 3};
		}
	}

	public static class MBeanWithDoubleArray implements ConcurrentJmxBean {

		@JmxAttribute
		public double[] getDoubleNumbers() {
			return new double[]{1.1, 2.2, 3.3};
		}
	}
	// endregion

	// region helper methods
	public static DynamicMBean createDynamicMBeanFor(Object... objects) {
		return DynamicMBeanFactory.create()
				.createDynamicMBean(asList(objects), defaultSettings(), false);
	}
	// endregion
}
