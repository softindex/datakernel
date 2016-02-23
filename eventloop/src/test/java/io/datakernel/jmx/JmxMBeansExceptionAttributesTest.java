///*
// * Copyright (C) 2015 SoftIndex LLC.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.datakernel.jmx;
//
//import org.junit.Test;
//
//import javax.management.DynamicMBean;
//import javax.management.MBeanAttributeInfo;
//import javax.management.MBeanException;
//import javax.management.MBeanInfo;
//import javax.management.openmbean.*;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.Executor;
//import java.util.concurrent.Executors;
//
//import static java.util.Arrays.asList;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//public class JmxMBeansExceptionAttributesTest {
//
//	@Test
//	public void itShouldProperlyReturnMBeanInfoAboutThrowables() throws Exception {
//		Exception notThrownException = new RuntimeException("ex-msg", new IllegalArgumentException());
//		MonitorableWithExceptionAttribute monitorable = new MonitorableWithExceptionAttribute(notThrownException);
//
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);
//		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
//		Map<String, MBeanAttributeInfo> nameToAttr = formNameToAttr(mBeanInfo.getAttributes());
//		assertEquals(1, nameToAttr.size());
//
//		MBeanAttributeInfo listAttr = nameToAttr.get("exceptionAttr");
//
//		String expectedTypeName = "CompositeDataOfThrowable";
//
//		assertEquals(expectedTypeName, listAttr.getType());
//		assertEquals(true, listAttr.isReadable());
//		assertEquals(false, listAttr.isWritable());
//	}
//
//	@Test
//	public void itShouldProperlyReturnThrowables() throws Exception {
//		Exception thrownException;
//		try {
//			throw new RuntimeException("ex-msg", new IllegalArgumentException());
//		} catch (Exception e) {
//			thrownException = e;
//		}
//		MonitorableWithExceptionAttribute monitorable = new MonitorableWithExceptionAttribute(thrownException);
//
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable), false);
//
//		CompositeData exceptionInfo = (CompositeData) mbean.getAttribute("exceptionAttr");
//
//		CompositeType expectedCompositeType = buildExcpectedCompositeTypeForThrowable();
//		assertEquals(expectedCompositeType, exceptionInfo.getCompositeType());
//
//		assertEquals("java.lang.RuntimeException", exceptionInfo.get("type"));
//		assertEquals("ex-msg", exceptionInfo.get("message"));
//		assertEquals("java.lang.IllegalArgumentException", exceptionInfo.get("cause"));
//		assertTrue(((String[]) exceptionInfo.get("stackTrace")).length > 0);
//	}
//
//	@Test(expected = MBeanException.class)
//	public void itShouldThrowMBeanExceptionIfThrowablesAreDifferentInInstancesOfPoolOfMonitorables() throws Exception {
//		MonitorableWithExceptionAttribute monitorable_1 =
//				new MonitorableWithExceptionAttribute(new IllegalArgumentException());
//		MonitorableWithExceptionAttribute monitorable_2 =
//				new MonitorableWithExceptionAttribute(new IllegalArgumentException());
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);
//
//		mbean.getAttribute("exceptionAttr");
//	}
//
//	@Test
//	public void itShouldReturnProperValueIfAllThrowablesAreSameForPoolOfMonitorables() throws Exception {
//		Exception exception = new Exception("msg");
//		MonitorableWithExceptionAttribute monitorable_1 = new MonitorableWithExceptionAttribute(exception);
//		MonitorableWithExceptionAttribute monitorable_2 = new MonitorableWithExceptionAttribute(exception);
//		DynamicMBean mbean = JmxMBeans.factory().createFor(asList(monitorable_1, monitorable_2), false);
//
//		CompositeData exceptionInfo = (CompositeData) mbean.getAttribute("exceptionAttr");
//		assertEquals("java.lang.Exception", exceptionInfo.get("type"));
//		assertEquals("msg", exceptionInfo.get("message"));
//	}
//
//	// helpers
//	public CompositeType buildExcpectedCompositeTypeForThrowable() throws OpenDataException {
//		String expectedTypeName = "CompositeDataOfThrowable";
//		String[] excpectedItemNames = new String[]{
//				"type",
//				"message",
//				"cause",
//				"stackTrace"
//		};
//		OpenType<?>[] expectedItemTypes = new OpenType<?>[]{
//				SimpleType.STRING,
//				SimpleType.STRING,
//				SimpleType.STRING,
//				new ArrayType<>(1, SimpleType.STRING)
//		};
//		CompositeType expectedCompositeType = new CompositeType(
//				expectedTypeName,
//				expectedTypeName,
//				excpectedItemNames,
//				excpectedItemNames,
//				expectedItemTypes);
//
//		return expectedCompositeType;
//	}
//
//	public static Map<String, MBeanAttributeInfo> formNameToAttr(MBeanAttributeInfo[] attributes) {
//		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
//		for (MBeanAttributeInfo attribute : attributes) {
//			nameToAttr.put(attribute.getName(), attribute);
//		}
//		return nameToAttr;
//	}
//
//	public static final class MonitorableWithExceptionAttribute implements ConcurrentJmxMBean {
//		private Throwable exception;
//
//		public MonitorableWithExceptionAttribute(Throwable exception) {
//			this.exception = exception;
//		}
//
//		@JmxAttribute
//		public Throwable getExceptionAttr() {
//			return exception;
//		}
//
//		@Override
//		public Executor getJmxExecutor() {
//			return Executors.newSingleThreadExecutor();
//		}
//	}
//}
