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
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxReducer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.management.DynamicMBean;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.jmx.MBeanSettings.defaultSettings;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DynamicMBeanFactoryImplAttributeReducersTest {

	public static final Eventloop EVENTLOOP = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

	// region simple type reducers
	@Test
	public void createdMBeanShouldUseSpecifiedReducerForAggregation() throws Exception {
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(
						asList(new MBeanWithCustomReducer(200), new MBeanWithCustomReducer(350)),
						defaultSettings(),
						false);

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

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return EVENTLOOP;
		}
	}

	public static final class ConstantValueReducer implements JmxReducer<Object> {
		public static final int CONSTANT_VALUE = 10;

		@Override
		public Object reduce(List<?> list) {
			return CONSTANT_VALUE;
		}
	}
	// simple type reducers

	// region pojo reducers
	@Test
	public void properlyAggregatesPojosWithReducer() throws Exception {
		MBeanWithPojoReducer mbean_1 = new MBeanWithPojoReducer(new PojoStub(10, "abc"));
		MBeanWithPojoReducer mbean_2 = new MBeanWithPojoReducer(new PojoStub(15, "xz"));
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(
						asList(mbean_1, mbean_2),
						defaultSettings(),
						false);

		assertEquals(25, mbean.getAttribute("pojo_count"));
		assertEquals("abcxz", mbean.getAttribute("pojo_name"));
	}

	public static final class MBeanWithPojoReducer implements EventloopJmxMBean {
		private final PojoStub pojo;

		public MBeanWithPojoReducer(PojoStub pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute(reducer = PojoStubReducer.class)
		public PojoStub getPojo() {
			return pojo;
		}

		@NotNull
		@Override
		public Eventloop getEventloop() {
			return EVENTLOOP;
		}
	}

	public static final class PojoStub {
		private final int count;
		private final String name;

		public PojoStub(int count, String name) {
			this.count = count;
			this.name = name;
		}

		@JmxAttribute
		public int getCount() {
			return count;
		}

		@JmxAttribute
		public String getName() {
			return name;
		}
	}

	public static final class PojoStubReducer implements JmxReducer<PojoStub> {
		@Override
		public PojoStub reduce(List<? extends PojoStub> list) {
			int totalCount = 0;
			String totalName = "";

			for (PojoStub pojoStub : list) {
				totalCount += pojoStub.getCount();
				totalName += pojoStub.getName();
			}

			return new PojoStub(totalCount, totalName);
		}
	}
	// endregion
}
