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
import org.junit.Test;

import javax.management.DynamicMBean;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JmxMBeansAttributeReducersTest {

	@Test
	public void createdMBeanShouldUseSpecifiedReducerForAggregation() throws Exception {
		DynamicMBean mbean = JmxMBeans.factory().createFor(
				asList(new MBeanWithCustomReducer(200), new MBeanWithCustomReducer(350)),
				false
		);

		assertEquals(ConstantValueReducer.CONSTANT_VALUE, mbean.getAttribute("attr"));
	}

	public static final class MBeanWithCustomReducer implements EventloopJmxMBean {
		private final int attr;

		public MBeanWithCustomReducer(int attr) {
			this.attr = attr;
		}

		@JmxAttribute(reducer = ConstantValueReducer.class)
		public int getAttr() {
			return attr;
		}

		@Override
		public Eventloop getEventloop() {
			return Eventloop.create();
		}
	}

	public static final class ConstantValueReducer implements JmxReducer<Object> {
		public static final int CONSTANT_VALUE = 10;

		@Override
		public Object reduce(List<?> input) {
			return CONSTANT_VALUE;
		}
	}
}
