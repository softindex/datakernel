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

package io.datakernel.cube;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationKeyRelationshipsTest {
	private AggregationKeyRelationships aggregationKeyRelationships;

	@Before
	public void setUp() throws Exception {
		aggregationKeyRelationships = new AggregationKeyRelationships(
				ImmutableMap.<String, String>builder()
						.put("campaign", "advertiser")
						.put("offer", "campaign")
						.put("goal", "offer")
						.put("banner", "offer")
						.put("keyword", "offer")
						.build());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDrillDownChains() throws Exception {
		Set<List<String>> drillDownChains1 = aggregationKeyRelationships.buildDrillDownChains(Sets.<String>newHashSet(), newHashSet("advertiser", "banner", "campaign", "offer"));
		assertEquals(newHashSet(newArrayList("advertiser"), newArrayList("advertiser", "campaign", "offer"), newArrayList("advertiser", "campaign", "offer", "banner"),
				newArrayList("advertiser", "campaign")), drillDownChains1);

		Set<List<String>> drillDownChains2 = aggregationKeyRelationships.buildDrillDownChains(Sets.<String>newHashSet(), newHashSet("banner", "campaign", "offer"));
		assertEquals(newHashSet(newArrayList("advertiser", "campaign", "offer", "banner"),
				newArrayList("advertiser", "campaign"), newArrayList("advertiser", "campaign", "offer")), drillDownChains2);
	}

	@Test
	public void testFindChildren() throws Exception {
		Set<String> goalChildren = aggregationKeyRelationships.findChildren("goal");
		assertTrue(goalChildren.isEmpty());

		Set<String> advertiserChildren = aggregationKeyRelationships.findChildren("advertiser");
		assertEquals(newHashSet("campaign", "offer", "goal", "banner", "keyword"), advertiserChildren);

		Set<String> campaignChildren = aggregationKeyRelationships.findChildren("campaign");
		assertEquals(newHashSet("offer", "goal", "banner", "keyword"), campaignChildren);
	}
}