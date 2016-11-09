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

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AggregationKeyRelationsTest {
	private Cube cube;

	@Before
	public void setUp() throws Exception {
		cube = Cube.createUninitialized()
				.withRelation("campaign", "advertiser")
				.withRelation("offer", "campaign")
				.withRelation("goal", "offer")
				.withRelation("banner", "offer")
				.withRelation("keyword", "offer");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDrillDownChains() throws Exception {
		Set<List<String>> drillDownChains1 = cube.buildDrillDownChains(Sets.<String>newHashSet(), newHashSet("advertiser", "banner", "campaign", "offer"));
		assertEquals(newHashSet(asList("advertiser"), asList("advertiser", "campaign", "offer"), asList("advertiser", "campaign", "offer", "banner"),
				asList("advertiser", "campaign")), drillDownChains1);

		Set<List<String>> drillDownChains2 = cube.buildDrillDownChains(Sets.<String>newHashSet(), newHashSet("banner", "campaign", "offer"));
		assertEquals(newHashSet(asList("advertiser", "campaign", "offer", "banner"),
				asList("advertiser", "campaign"), asList("advertiser", "campaign", "offer")), drillDownChains2);
	}

}