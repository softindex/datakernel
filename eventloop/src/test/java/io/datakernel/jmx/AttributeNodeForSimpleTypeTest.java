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

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AttributeNodeForSimpleTypeTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	ValueFetcher fetcher = context.mock(ValueFetcher.class);

	@Test
	public void returnsProperOpenType() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		AttributeNodeForSimpleType attrNodeDouble = new AttributeNodeForSimpleType("doubleAttr", fetcher, double.class);
		AttributeNodeForSimpleType attrNodeString = new AttributeNodeForSimpleType("stringAttr", fetcher, String.class);

		assertEquals(SimpleType.INTEGER, attrNodeInt.getOpenType());
		assertEquals(SimpleType.DOUBLE, attrNodeDouble.getOpenType());
		assertEquals(SimpleType.STRING, attrNodeString.getOpenType());
	}

	@Test
	public void returnsProperFlattenedMapWithOneEntry() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);

		Map<String, OpenType<?>> map = attrNodeInt.getFlattenedOpenTypes();
		assertEquals(1, map.size());
		assertEquals(SimpleType.INTEGER, map.get("intAttr"));
	}

	@Test
	public void isNotRefreshable() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);

		assertFalse(attrNodeInt.isRefreshable());
	}

	// aggregation tests
	@Test
	public void returnsCommonValueIfAttrValuesInSourcesAreEqual() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		final AttributeSource attrSrc_1 = new AttributeSource(100);
		final AttributeSource attrSrc_2 = new AttributeSource(100);
		List<AttributeSource> sources = asList(attrSrc_1, attrSrc_2);
		prepareFetcher(attrSrc_1, attrSrc_2);

		assertEquals(100, attrNodeInt.aggregateAttribute("intAttr", sources));
	}

	@Test
	public void returnsNullIfAttrValuesInSourcesAreDifferent() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		final AttributeSource attrSrc_1 = new AttributeSource(100);
		final AttributeSource attrSrc_2 = new AttributeSource(250);
		List<AttributeSource> sources = asList(attrSrc_1, attrSrc_2);
		prepareFetcher(attrSrc_1, attrSrc_2);

		assertEquals(null, attrNodeInt.aggregateAttribute("intAttr", sources));
	}

	@Test(expected = IllegalArgumentException.class)
	public void throwsExceptionIfAttrNameIsIncorrect() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		final AttributeSource attrSrc_1 = new AttributeSource(100);
		final AttributeSource attrSrc_2 = new AttributeSource(250);
		List<AttributeSource> sources = asList(attrSrc_1, attrSrc_2);
		prepareFetcher(attrSrc_1, attrSrc_2);

		assertEquals(null, attrNodeInt.aggregateAttribute("incorectName", sources));
	}

	@Test
	public void ignoresNullSources() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		final AttributeSource attrSrc_1 = new AttributeSource(150);
		final AttributeSource attrSrc_2 = null;
		final AttributeSource attrSrc_3 = new AttributeSource(150);
		List<AttributeSource> sources = asList(attrSrc_1, attrSrc_2, attrSrc_3);
		prepareFetcher(attrSrc_1, attrSrc_3);

		assertEquals(150, attrNodeInt.aggregateAttribute("intAttr", sources));
	}

	@Test
	public void returnsNullIfAllSourcesAreNull() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		final AttributeSource attrSrc_1 = null;
		final AttributeSource attrSrc_2 = null;
		final AttributeSource attrSrc_3 = null;
		List<AttributeSource> sources = asList(attrSrc_1, attrSrc_2, attrSrc_3);

		assertEquals(null, attrNodeInt.aggregateAttribute("intAttr", sources));
	}

	@Test
	public void returnsNullIfSourcesListIsEmpty() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		List<AttributeSource> sources = new ArrayList<>();

		assertEquals(null, attrNodeInt.aggregateAttribute("intAttr", sources));
	}

	@Test
	public void properlyCreatesMapWithNameAndAggregatedValue() {
		AttributeNodeForSimpleType attrNodeInt = new AttributeNodeForSimpleType("intAttr", fetcher, int.class);
		final AttributeSource attrSrc_1 = new AttributeSource(100);
		final AttributeSource attrSrc_2 = new AttributeSource(100);
		List<AttributeSource> sources = asList(attrSrc_1, attrSrc_2);
		prepareFetcher(attrSrc_1, attrSrc_2);

		Map<String, Object> allAttrs = attrNodeInt.aggregateAllAttributes(sources);
		assertEquals(1, allAttrs.size());
		assertEquals(100, allAttrs.get("intAttr"));
	}

	// helper methods

	public void prepareFetcher(final AttributeSource src1, final AttributeSource src2) {
		context.checking(new Expectations() {{
			allowing(fetcher).fetchFrom(src1);
			will(returnValue(src1.getValue()));

			allowing(fetcher).fetchFrom(src2);
			will(returnValue(src2.getValue()));
		}});
	}

	// helper classes

	public static final class AttributeSource {
		private final int value;

		public AttributeSource(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}
}
