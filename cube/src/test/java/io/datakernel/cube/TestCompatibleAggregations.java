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

package io.datakernel.cube;

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.aggregation.AggregationPredicates;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static io.datakernel.aggregation.AggregationPredicates.*;
import static io.datakernel.aggregation.AggregationUtils.valuesToLinkedMap;
import static io.datakernel.aggregation.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation.measure.Measures.*;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static java.util.Arrays.asList;
import static java.util.stream.Stream.of;
import static org.junit.Assert.*;

public class TestCompatibleAggregations {
	private static final Map<String, String> DATA_ITEM_DIMENSIONS = valuesToLinkedMap(of(
			new AbstractMap.SimpleEntry<>("date", "date"),
			new AbstractMap.SimpleEntry<>("advertiser", "advertiser"),
			new AbstractMap.SimpleEntry<>("campaign", "campaign"),
			new AbstractMap.SimpleEntry<>("banner", "banner"),
			new AbstractMap.SimpleEntry<>("affiliate", "affiliate"),
			new AbstractMap.SimpleEntry<>("site", "site"),
			new AbstractMap.SimpleEntry<>("placement", "placement")));

	private static final Map<String, String> DATA_ITEM_MEASURES = valuesToLinkedMap(of(
			new AbstractMap.SimpleEntry<>("eventCount", "null"),
			new AbstractMap.SimpleEntry<>("minRevenue", "revenue"),
			new AbstractMap.SimpleEntry<>("maxRevenue", "revenue"),
			new AbstractMap.SimpleEntry<>("revenue", "revenue"),
			new AbstractMap.SimpleEntry<>("impressions", "impressions"),
			new AbstractMap.SimpleEntry<>("clicks", "clicks"),
			new AbstractMap.SimpleEntry<>("conversions", "conversions"),
			new AbstractMap.SimpleEntry<>("uniqueUserIdsCount", "userId"),
			new AbstractMap.SimpleEntry<>("errors", "errors")));

	private static final Map<String, FieldType> DIMENSIONS_DAILY_AGGREGATION = valuesToLinkedMap(of(
			new AbstractMap.SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01")))));

	private static final Map<String, FieldType> DIMENSIONS_ADVERTISERS_AGGREGATION = valuesToLinkedMap(of(
			new AbstractMap.SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			new AbstractMap.SimpleEntry<>("advertiser", ofInt()),
			new AbstractMap.SimpleEntry<>("campaign", ofInt()),
			new AbstractMap.SimpleEntry<>("banner", ofInt())));

	private static final Map<String, FieldType> DIMENSIONS_AFFILIATES_AGGREGATION = valuesToLinkedMap(of(
			new AbstractMap.SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			new AbstractMap.SimpleEntry<>("affiliate", ofInt()),
			new AbstractMap.SimpleEntry<>("site", ofString())));

	private static final Map<String, FieldType> DIMENSIONS_DETAILED_AFFILIATES_AGGREGATION = valuesToLinkedMap(of(
			new AbstractMap.SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			new AbstractMap.SimpleEntry<>("affiliate", ofInt()),
			new AbstractMap.SimpleEntry<>("site", ofString()),
			new AbstractMap.SimpleEntry<>("placement", ofInt())));

	private static final Map<String, Measure> MEASURES = valuesToLinkedMap(of(
			new AbstractMap.SimpleEntry<>("impressions", sum(ofLong())),
			new AbstractMap.SimpleEntry<>("clicks", sum(ofLong())),
			new AbstractMap.SimpleEntry<>("conversions", sum(ofLong())),
			new AbstractMap.SimpleEntry<>("revenue", sum(ofDouble())),
			new AbstractMap.SimpleEntry<>("eventCount", count(ofInt())),
			new AbstractMap.SimpleEntry<>("minRevenue", min(ofDouble())),
			new AbstractMap.SimpleEntry<>("maxRevenue", max(ofDouble())),
			new AbstractMap.SimpleEntry<>("uniqueUserIdsCount", hyperLogLog(1024)),
			new AbstractMap.SimpleEntry<>("errors", sum(ofLong()))));

	private static final AggregationPredicate DAILY_AGGREGATION_PREDICATE = alwaysTrue();
	private static final Cube.AggregationConfig DAILY_AGGREGATION = id("daily")
			.withDimensions(DIMENSIONS_DAILY_AGGREGATION.keySet())
			.withMeasures(MEASURES.keySet())
			.withPredicate(DAILY_AGGREGATION_PREDICATE);

	private static final int EXCLUDE_AFFILIATE = 0;
	private static final String EXCLUDE_SITE = "--";
	private static final Object EXCLUDE_PLACEMENT = 0;

	private static final int EXCLUDE_ADVERTISER = 0;
	private static final int EXCLUDE_CAMPAIGN = 0;
	private static final int EXCLUDE_BANNER = 0;

	private static final AggregationPredicate ADVERTISER_AGGREGATION_PREDICATE =
			and(not(eq("advertiser", EXCLUDE_ADVERTISER)), not(eq("banner", EXCLUDE_BANNER)), not(eq("campaign", EXCLUDE_CAMPAIGN)));
	private static final Cube.AggregationConfig ADVERTISERS_AGGREGATION = id("advertisers")
			.withDimensions(DIMENSIONS_ADVERTISERS_AGGREGATION.keySet())
			.withMeasures(MEASURES.keySet())
			.withPredicate(ADVERTISER_AGGREGATION_PREDICATE);

	private static final AggregationPredicate AFFILIATES_AGGREGATION_PREDICATE =
			and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)));
	private static final Cube.AggregationConfig AFFILIATES_AGGREGATION = id("affiliates")
			.withDimensions(DIMENSIONS_AFFILIATES_AGGREGATION.keySet())
			.withMeasures(MEASURES.keySet())
			.withPredicate(AFFILIATES_AGGREGATION_PREDICATE);

	private static final AggregationPredicate DETAILED_AFFILIATES_AGGREGATION_PREDICATE =
			and(notEq("affiliate", EXCLUDE_AFFILIATE), not(eq("site", EXCLUDE_SITE)), not(eq("placement", EXCLUDE_PLACEMENT)));
	private static final Cube.AggregationConfig DETAILED_AFFILIATES_AGGREGATION = id("detailed_affiliates")
			.withDimensions(DIMENSIONS_DETAILED_AFFILIATES_AGGREGATION.keySet())
			.withMeasures(MEASURES.keySet())
			.withPredicate(DETAILED_AFFILIATES_AGGREGATION_PREDICATE);

	private static final AggregationPredicate LIMITED_DATES_AGGREGATION_PREDICATE =
			and(between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2010-01-01")));
	private static final Cube.AggregationConfig LIMITED_DATES_AGGREGATION = id("limited_date")
			.withDimensions(DIMENSIONS_DAILY_AGGREGATION.keySet())
			.withMeasures(MEASURES.keySet())
			.withPredicate(LIMITED_DATES_AGGREGATION_PREDICATE);

	private Cube cube;
	private Cube cubeWithDetailedAggregation;

	@Before
	public void setUp() {
		cube = Cube.createUninitialized()
				.withMeasures(MEASURES)

				.withDimensions(DIMENSIONS_DAILY_AGGREGATION)
				.withDimensions(DIMENSIONS_ADVERTISERS_AGGREGATION)
				.withDimensions(DIMENSIONS_AFFILIATES_AGGREGATION)

				.withAggregations(asList(DAILY_AGGREGATION, ADVERTISERS_AGGREGATION, AFFILIATES_AGGREGATION));

		cubeWithDetailedAggregation = Cube.createUninitialized()
				.withMeasures(MEASURES)

				.withDimensions(DIMENSIONS_DAILY_AGGREGATION)
				.withDimensions(DIMENSIONS_ADVERTISERS_AGGREGATION)
				.withDimensions(DIMENSIONS_AFFILIATES_AGGREGATION)
				.withDimensions(DIMENSIONS_DETAILED_AFFILIATES_AGGREGATION)

				.withAggregations(asList(DAILY_AGGREGATION, ADVERTISERS_AGGREGATION, AFFILIATES_AGGREGATION))
				.withAggregation(DETAILED_AFFILIATES_AGGREGATION)
				.withAggregation(LIMITED_DATES_AGGREGATION.withPredicate(LIMITED_DATES_AGGREGATION_PREDICATE));
	}

	// region test getCompatibleAggregationsForQuery for data input
	@Test
	public void withAlwaysTrueDataPredicate_MatchesAllAggregations() {
		final AggregationPredicate dataPredicate = alwaysTrue();
		Set<String> compatibleAggregations = cube.getCompatibleAggregationsForDataInput(
				DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate).keySet();

		assertEquals(3, compatibleAggregations.size());
		assertTrue(compatibleAggregations.contains(DAILY_AGGREGATION.getId()));
		assertTrue(compatibleAggregations.contains(ADVERTISERS_AGGREGATION.getId()));
		assertTrue(compatibleAggregations.contains(AFFILIATES_AGGREGATION.getId()));
	}

	@Test
	public void withCompatibleDataPredicate_MatchesAggregationWithPredicateThatSubsetOfDataPredicate2() {
		final AggregationPredicate dataPredicate = and(notEq("affiliate", EXCLUDE_AFFILIATE), notEq("site", EXCLUDE_SITE));
		Map<String, AggregationPredicate> compatibleAggregationsWithFilterPredicate = cube.getCompatibleAggregationsForDataInput(
				DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate);

		assertEquals(3, compatibleAggregationsWithFilterPredicate.size());

		// matches aggregation with optimization
		// (if dataPredicate equals aggregationPredicate -> do not use stream filter)
		assertTrue(compatibleAggregationsWithFilterPredicate.containsKey(AFFILIATES_AGGREGATION.getId()));
		assertEquals(alwaysTrue(), compatibleAggregationsWithFilterPredicate.get(AFFILIATES_AGGREGATION.getId()));

		assertTrue(compatibleAggregationsWithFilterPredicate.containsKey(ADVERTISERS_AGGREGATION.getId()));
		assertEquals(ADVERTISER_AGGREGATION_PREDICATE.simplify(), compatibleAggregationsWithFilterPredicate.get(ADVERTISERS_AGGREGATION.getId()));

		assertTrue(compatibleAggregationsWithFilterPredicate.containsKey(DAILY_AGGREGATION.getId()));
		assertEquals(alwaysTrue(), compatibleAggregationsWithFilterPredicate.get(DAILY_AGGREGATION.getId()));
	}

	@Test
	public void withIncompatibleDataPredicate_DoesNotMatchAggregationWithLimitedDateRange() {
		final AggregationPredicate dataPredicate = and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)),
				between("date", LocalDate.parse("2012-01-01"), LocalDate.parse("2016-01-01")));
		Set<String> compatibleAggregations = cubeWithDetailedAggregation.getCompatibleAggregationsForDataInput(
				DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate).keySet();

		assertFalse(compatibleAggregations.contains(LIMITED_DATES_AGGREGATION.getId()));
	}

	@Test
	public void withSubsetBetweenDataPredicate_MatchesAggregation() {
		final AggregationPredicate dataPredicate = and(notEq("date", LocalDate.parse("2001-01-04")),
				between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01")));

		Map<String, AggregationPredicate> compatibleAggregations = cubeWithDetailedAggregation.getCompatibleAggregationsForDataInput(
				DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate);

		//matches all aggregations, but with different filtering logic
		assertTrue(compatibleAggregations.containsKey(LIMITED_DATES_AGGREGATION.getId()));
		assertEquals(LIMITED_DATES_AGGREGATION_PREDICATE.simplify(), compatibleAggregations.get(LIMITED_DATES_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(DAILY_AGGREGATION.getId()));
		assertEquals(AggregationPredicates.alwaysTrue(), compatibleAggregations.get(DAILY_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(ADVERTISERS_AGGREGATION.getId()));
		assertEquals(ADVERTISER_AGGREGATION_PREDICATE.simplify(), compatibleAggregations.get(ADVERTISERS_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(AFFILIATES_AGGREGATION.getId()));
		assertEquals(AFFILIATES_AGGREGATION_PREDICATE.simplify(), compatibleAggregations.get(AFFILIATES_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(DETAILED_AFFILIATES_AGGREGATION.getId()));
		assertEquals(DETAILED_AFFILIATES_AGGREGATION_PREDICATE.simplify(), compatibleAggregations.get(DETAILED_AFFILIATES_AGGREGATION.getId()));
	}
	// endregion

	// region test getCompatibleAggregationsForQuery for query
	@Test
	public void withWherePredicateAlwaysTrue_MatchesDailyAggregation() {
		final AggregationPredicate whereQueryPredicate = alwaysTrue();

		List<Cube.AggregationContainer> actualAggregations = cube.getCompatibleAggregationsForQuery(
				asList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cube.getAggregation(DAILY_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForAdvertisersAggregation_MatchesAdvertisersAggregation() {
		final AggregationPredicate whereQueryPredicate = and(
				not(eq("advertiser", EXCLUDE_AFFILIATE)),
				not(eq("campaign", EXCLUDE_CAMPAIGN)),
				not(eq("banner", EXCLUDE_BANNER)));

		List<Cube.AggregationContainer> actualAggregations = cube.getCompatibleAggregationsForQuery(
				asList("advertiser", "campaign", "banner"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cube.getAggregation(ADVERTISERS_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForAffiliatesAggregation_MatchesAffiliatesAggregation() {
		final AggregationPredicate whereQueryPredicate = and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)));

		List<Cube.AggregationContainer> actualAggregations = cube.getCompatibleAggregationsForQuery(
				asList("affiliate", "site"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cube.getAggregation(AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForBothAffiliatesAggregations_MatchesAffiliatesAggregation() {
		final AggregationPredicate whereQueryPredicate = and(
				not(eq("affiliate", EXCLUDE_AFFILIATE)),
				not(eq("site", EXCLUDE_SITE)),
				not(eq("placement", EXCLUDE_PLACEMENT)));

		List<Cube.AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						asList("affiliate", "site", "placement"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DETAILED_AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForDetailedAffiliatesAggregations_MatchesDetailedAffiliatesAggregation() {
		final AggregationPredicate whereQueryPredicate = and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)), not(eq("placement", EXCLUDE_PLACEMENT)));

		List<Cube.AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						asList("affiliate", "site", "placement"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DETAILED_AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForDailyAggregation_MatchesOnlyDailyAggregations() {
		final AggregationPredicate whereQueryPredicate = between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01"));

		List<Cube.AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						asList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DAILY_AGGREGATION.getId());
		Aggregation expected2 = cubeWithDetailedAggregation.getAggregation(LIMITED_DATES_AGGREGATION.getId());

		assertEquals(2, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
		assertEquals(expected2.toString(), actualAggregations.get(1).toString());
	}

	@Test
	public void withWherePredicateForAdvertisersAggregation_MatchesOneAggregation() {
		final AggregationPredicate whereQueryPredicate = and(
				not(eq("advertiser", EXCLUDE_ADVERTISER)), not(eq("campaign", EXCLUDE_CAMPAIGN)), not(eq("banner", EXCLUDE_BANNER)),
				between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01")));

		List<Cube.AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						asList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(ADVERTISERS_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForDailyAggregation_MatchesTwoAggregations() {
		final AggregationPredicate whereQueryPredicate = eq("date", LocalDate.parse("2001-01-01"));

		List<Cube.AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						asList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DAILY_AGGREGATION.getId());
		Aggregation expected2 = cubeWithDetailedAggregation.getAggregation(LIMITED_DATES_AGGREGATION.getId());

		assertEquals(2, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
		assertEquals(expected2.toString(), actualAggregations.get(1).toString());
	}
	//endregion

}
