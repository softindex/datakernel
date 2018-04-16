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

import javax.management.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.jmx.MBeanSettings.defaultSettings;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxMBeansAttributesBulkGettersTest {
	private static final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

	@Test
	public void bulkGetOmitsAttributesWithExceptionButReturnsValidAttributes() {
		DynamicMBean mbean = JmxMBeans.factory().createFor(singletonList(new MBeanStub()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> attrs = io.datakernel.jmx.helper.Utils.nameToAttribute(mbean.getMBeanInfo().getAttributes());

		String[] expectedAttrNames = new String[]{"text", "value", "number"};
		assertEquals(new HashSet<>(asList(expectedAttrNames)), attrs.keySet());

		AttributeList fetchedAttrs = mbean.getAttributes(expectedAttrNames);
		assertEquals(2, fetchedAttrs.size());

		Map<String, Object> attrNameToValue = nameToAttribute(fetchedAttrs);

		assertTrue(attrNameToValue.containsKey("text"));
		assertEquals("data", attrNameToValue.get("text"));

		assertTrue(attrNameToValue.containsKey("number"));
		assertEquals(100L, attrNameToValue.get("number"));
	}

	@Test(expected = MBeanException.class)
	public void propagatesExceptionInCaseOfSingleAttributeGet() throws Exception {
		DynamicMBean mbean = JmxMBeans.factory().createFor(singletonList(new MBeanStub()), defaultSettings(), false);

		mbean.getAttribute("value");
	}

	public static class CustomException extends RuntimeException {}

	public static final class MBeanStub implements EventloopJmxMBean {

		@JmxAttribute
		public String getText() {
			return "data";
		}

		@JmxAttribute
		public int getValue() {
			throw new CustomException();
		}

		@JmxAttribute
		public long getNumber() {
			return 100L;
		}

		@Override
		public Eventloop getEventloop() {
			return eventloop;
		}
	}

	private static Map<String, Object> nameToAttribute(AttributeList fetchedAttrs) {
		Map<String, Object> nameToValue = new HashMap<>();
		for (Object fetchAttr : fetchedAttrs) {
			Attribute attr = (Attribute) fetchAttr;
			nameToValue.put(attr.getName(), attr.getValue());
		}
		return nameToValue;
	}
}
