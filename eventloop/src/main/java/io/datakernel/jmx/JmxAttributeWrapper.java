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
//import java.util.SortedMap;
//
//public final class JmxAttributeWrapper {
//
//	private final SortedMap<String, TypeAndValue> attributes;
//
//	private JmxAttributeWrapper(SortedMap<String, TypeAndValue> attributes) {
//		this.attributes = attributes;
//	}
//
//	public SortedMap<String, TypeAndValue> getAttributes() {
//		return null;
//	}
//
//	public static JmxAttributeWrapper wrap(Object attribute, String name) {
//		/*
//		cases:
//
//		0)
//		JmxAttributeWrapper ???
//
//		I)
//		JmxStats
//
//		II)
//		boolean
//		int
//		long
//		double
//		String
//
//		III)
//		List
//
//		IV)
//		Array)
//
//		V)
//		Throwable
//
//		VI)
//		Arbitrary POJO
//
//		 */
//
//		if (JmxStats.class.isAssignableFrom(attribute.getClass())) {
//			JmxStats<?> jmxStats = (JmxStats<?>) attribute;
//			return new JmxAttributeWrapper(jmxStats.getAttributes());
//		}
//
//		return null;
//	}
//
//	private static SortedMap<String, TypeAndValue> extractAttributeFromPOJO(Object pojo) {
//
//	}
//
//	private static SortedMap<String, TypeAndValue> addPrimaryName(SortedMap<String, TypeAndValue> srcAttributes) {
//		return null;
//	}
//}
