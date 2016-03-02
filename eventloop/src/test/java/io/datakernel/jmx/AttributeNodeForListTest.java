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
//import org.jmock.Expectations;
//import org.jmock.integration.junit4.JUnitRuleMockery;
//import org.junit.Rule;
//import org.junit.Test;
//
//import javax.management.openmbean.ArrayType;
//import javax.management.openmbean.OpenDataException;
//import javax.management.openmbean.OpenType;
//import javax.management.openmbean.SimpleType;
//
//import java.util.List;
//import java.util.Map;
//
//import static java.util.Arrays.asList;
//import static org.junit.Assert.assertEquals;
//
//public class AttributeNodeForListTest {
//	@Rule
//	public JUnitRuleMockery context = new JUnitRuleMockery();
//	ValueFetcher fetcher = new ListFetcher();
//
//	@Test
//	public void createsProperOpenType() throws OpenDataException {
//		AttributeNode stringSubNode = prepareFakeStringSubNode();
//		AttributeNodeForList attrNode = new AttributeNodeForList("listAttr", fetcher, stringSubNode);
//
//		OpenType<?> expectedType = new ArrayType<>(1, SimpleType.STRING);
//		assertEquals(expectedType, attrNode.getOpenType());
//	}
//
//	@Test
//	public void createsProperFlattenedMapWithOneEntry() throws OpenDataException {
//		AttributeNode stringSubNode = prepareFakeStringSubNode();
//		AttributeNodeForList attrNode = new AttributeNodeForList("listAttr", fetcher, stringSubNode);
//
//		Map<String, OpenType<?>> map = attrNode.getFlattenedOpenTypes();
//
//		assertEquals(1, map.size());
//
//		OpenType<?> expectedType = new ArrayType<>(1, SimpleType.STRING);
//		assertEquals(expectedType, map.get("listAttr"));
//	}
//
//	@Test
//	public void concatenatesListsFromSourcesDuringAggregation() {
//		AttributeNode stringSubNode = prepareFakeStringSubNode();
//		AttributeNodeForList attrNode = new AttributeNodeForList("listAttr", fetcher, stringSubNode);
//
//		ListAttrSource<String> src1 = new ListAttrSource<>(asList("a", "b"));
//		ListAttrSource<String> src2 = new ListAttrSource<>(asList("c"));
//
//		context.checking(new Expectations(){{
//
//		}});
//
//		assertEquals(new String[]{"a", "b", "c"}, attrNode.aggregateAttribute("listAttr", asList(src1, src2)));
//	}
//
//	// helper methods
//
//	public AttributeNode prepareFakeStringSubNode() {
//		final AttributeNode subNode = context.mock(AttributeNode.class);
//		context.checking(new Expectations(){{
//			allowing(subNode).getOpenType();
//			will(returnValue(SimpleType.STRING));
//
//			allowing(subNode).isRefreshable();
//			will(returnValue(false));
//		}});
//		return subNode;
//	}
//
//	// helper classes
//
//	public static final class ListAttrSource<T> {
//		private final List<T> listAttr;
//
//		public ListAttrSource(List<T> listAttr) {
//			this.listAttr = listAttr;
//		}
//
//		public List<T> getListAttr() {
//			return listAttr;
//		}
//	}
//
//	public static final class ListFetcher implements ValueFetcher {
//
//		@Override
//		public Object fetchFrom(Object source) {
//			return ((ListAttrSource) source).getListAttr();
//		}
//	}
//}
