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

package io.datakernel.cube.http;

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.aggregation.annotation.Key;
import io.datakernel.aggregation.annotation.Measures;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.async.AssertingResultCallback;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.*;
import io.datakernel.cube.attributes.AbstractAttributeResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogToCubeMetadataStorage;
import io.datakernel.logfs.LogToCubeRunner;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducers;
import org.joda.time.LocalDate;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static io.datakernel.aggregation.AggregationPredicates.*;
import static io.datakernel.aggregation.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation.measure.Measures.*;
import static io.datakernel.cube.ComputedMeasures.*;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.CubeQuery.Ordering.asc;
import static io.datakernel.cube.CubeTestUtils.*;
import static io.datakernel.cube.ReportType.DATA;
import static io.datakernel.cube.ReportType.DATA_WITH_TOTALS;
import static io.datakernel.cube.http.ReportingTest.LogItem.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReportingTest {
	public static final double DELTA = 1E-3;

	private static Configuration jooqConfiguration;
	private Eventloop eventloop;
	private AsyncHttpServer cubeHttpServer;
	private AsyncHttpClient httpClient;
	private CubeHttpClient cubeHttpClient;
	private Cube cube;
	private DefiningClassLoader classLoader;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String DATABASE_PROPERTIES_PATH = "test.properties";
	private static final SQLDialect DATABASE_DIALECT = SQLDialect.MYSQL;
	private static final String LOG_PARTITION_NAME = "partitionA";
	private static final List<String> LOG_PARTITIONS = singletonList(LOG_PARTITION_NAME);
	private static final String LOG_NAME = "testlog";

	private static final int SERVER_PORT = 50001;

	private static final Map<String, FieldType> DIMENSIONS_CUBE = ImmutableMap.<String, FieldType>builder()
			.put("date", ofLocalDate(LocalDate.parse("2000-01-01")))
			.put("advertiser", ofInt())
			.put("campaign", ofInt())
			.put("banner", ofInt())
			.put("affiliate", ofInt())
			.put("site", ofString())
			.build();

	private static final Map<String, FieldType> DIMENSIONS_DATE_AGGREGATION = ImmutableMap.<String, FieldType>builder()
			.put("date", ofLocalDate(LocalDate.parse("2000-01-01")))
			.build();

	private static final Map<String, FieldType> DIMENSIONS_ADVERTISERS_AGGREGATION = ImmutableMap.<String, FieldType>builder()
			.put("date", ofLocalDate(LocalDate.parse("2000-01-01")))
			.put("advertiser", ofInt())
			.put("campaign", ofInt())
			.put("banner", ofInt())
			.build();

	private static final Map<String, FieldType> DIMENSIONS_AFFILIATES_AGGREGATION = ImmutableMap.<String, FieldType>builder()
			.put("date", ofLocalDate(LocalDate.parse("2000-01-01")))
			.put("affiliate", ofInt())
			.put("site", ofString())
			.build();

	private static final Map<String, Measure> MEASURES = ImmutableMap.<String, Measure>builder()
			.put("impressions", sum(ofLong()))
			.put("clicks", sum(ofLong()))
			.put("conversions", sum(ofLong()))
			.put("revenue", sum(ofDouble()))
			.put("eventCount", count(ofInt()))
			.put("minRevenue", min(ofDouble()))
			.put("maxRevenue", max(ofDouble()))
			.put("uniqueUserIdsCount", hyperLogLog(1024))
			.put("errors", sum(ofLong()))
			.build();

	private static class AdvertiserResolver extends AbstractAttributeResolver<Integer, String> {
		@Override
		public Class<?>[] getKeyTypes() {
			return new Class[]{int.class};
		}

		@Override
		protected Integer toKey(Object[] keyArray) {
			return (int) keyArray[0];
		}

		@Override
		public Map<String, Class<?>> getAttributeTypes() {
			return ImmutableMap.<String, Class<?>>of("name", String.class);
		}

		@Override
		protected Object[] toAttributes(String attributes) {
			return new Object[]{attributes};
		}

		@Override
		public String resolveAttributes(Integer key) {
			switch (key) {
				case 1:
					return "first";
				case 2:
					return null;
				case 3:
					return "third";
				default:
					return null;
			}
		}
	}

	@Measures({"eventCount"})
	public static class LogItem {
		static final int EXCLUDE_AFFILIATE = 0;
		static final String EXCLUDE_SITE = "--";

		static final int EXCLUDE_ADVERTISER = 0;
		static final int EXCLUDE_CAMPAIGN = 0;
		static final int EXCLUDE_BANNER = 0;

		@Key
		@Serialize(order = 0)
		public int date;

		@Key
		@Serialize(order = 1)
		public int advertiser;

		@Key
		@Serialize(order = 2)
		public int campaign;

		@Key
		@Serialize(order = 3)
		public int banner;

		@Key
		@Serialize(order = 10)
		public int affiliate;

		@Key
		@Serialize(order = 11)
		public String site;

		@io.datakernel.aggregation.annotation.Measure
		@Serialize(order = 4)
		public long impressions;

		@io.datakernel.aggregation.annotation.Measure
		@Serialize(order = 5)
		public long clicks;

		@io.datakernel.aggregation.annotation.Measure
		@Serialize(order = 6)
		public long conversions;

		@Measures({"minRevenue", "maxRevenue", "revenue"})
		@Serialize(order = 7)
		public double revenue;

		@io.datakernel.aggregation.annotation.Measure("uniqueUserIdsCount")
		@Serialize(order = 8)
		public int userId;

		@io.datakernel.aggregation.annotation.Measure
		@Serialize(order = 9)
		public int errors;

		public LogItem() {
		}

		public LogItem(int date, int advertiser, int campaign, int banner, long impressions, long clicks,
		               long conversions, double revenue, int userId, int errors, int affiliate, String site) {
			this.date = date;
			this.advertiser = advertiser;
			this.campaign = campaign;
			this.banner = banner;
			this.impressions = impressions;
			this.clicks = clicks;
			this.conversions = conversions;
			this.revenue = revenue;
			this.userId = userId;
			this.errors = errors;
			this.affiliate = affiliate;
			this.site = site;
		}
	}

	public static class LogItemSplitter extends AggregatorSplitter<LogItem> {
		private static final AggregatorSplitter.Factory<LogItem> FACTORY = new AggregatorSplitter.Factory<LogItem>() {
			@Override
			public AggregatorSplitter<LogItem> create(Eventloop eventloop) {
				return new LogItemSplitter(eventloop);
			}
		};

		public LogItemSplitter(Eventloop eventloop) {
			super(eventloop);
		}

		public static Factory<LogItem> factory() {
			return FACTORY;
		}

		private StreamDataReceiver<LogItem> dateAggregator;
		private StreamDataReceiver<LogItem> dateAggregator2;

		@Override
		protected void addOutputs() {
			dateAggregator = addOutput(LogItem.class, and(notEq("advertiser", EXCLUDE_ADVERTISER), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER)));
			dateAggregator2 = addOutput(LogItem.class, and(notEq("affiliate", EXCLUDE_AFFILIATE), notEq("site", EXCLUDE_SITE)));
		}

		@Override
		protected void processItem(LogItem item) {
			if (item.advertiser != EXCLUDE_ADVERTISER && item.campaign != EXCLUDE_CAMPAIGN && item.banner != EXCLUDE_BANNER) {
				dateAggregator.onData(item);
			} else if (item.affiliate != 0 && !EXCLUDE_SITE.equals(item.site)) {
				dateAggregator2.onData(item);
			}
		}
	}

	@BeforeClass
	public static void initJooqConfiguration() throws IOException {
		jooqConfiguration = getJooqConfiguration(DATABASE_PROPERTIES_PATH, DATABASE_DIALECT);
	}

	@Before
	public void setUp() throws IOException {
		ExecutorService executor = Executors.newCachedThreadPool();

		classLoader = DefiningClassLoader.create();
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		AggregationChunkStorage aggregationChunkStorage =
				LocalFsChunkStorage.create(eventloop, executor, aggregationsDir);
		CubeMetadataStorageSql cubeMetadataStorageSql =
				CubeMetadataStorageSql.create(eventloop, executor, jooqConfiguration, "processId");

		cube = Cube.create(eventloop, executor, classLoader, cubeMetadataStorageSql, aggregationChunkStorage)
				.withClassLoaderCache(CubeClassLoaderCache.create(classLoader, 5))
				.withDimensions(DIMENSIONS_CUBE)
				.withMeasures(MEASURES)
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign")
				.withRelation("site", "affiliate")
				.withAttribute("advertiser.name", new AdvertiserResolver())
				.withComputedMeasure("ctr", percent(measure("clicks"), measure("impressions")))
				.withComputedMeasure("uniqueUserPercent", percent(div(measure("uniqueUserIdsCount"), measure("eventCount"))))
				.withComputedMeasure("errorsPercent", percent(div(measure("errors"), measure("impressions"))))

				.withAggregation(id("advertisers")
						.withDimensions(DIMENSIONS_ADVERTISERS_AGGREGATION.keySet())
						.withMeasures(MEASURES.keySet())
						.withPredicate(and(notEq("advertiser", EXCLUDE_ADVERTISER), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER))))

				.withAggregation(id("affiliates")
						.withDimensions(DIMENSIONS_AFFILIATES_AGGREGATION.keySet())
						.withMeasures(MEASURES.keySet())
						.withPredicate(and(notEq("affiliate", 0), notEq("site", EXCLUDE_SITE))))

				.withAggregation(id("daily")
						.withDimensions(DIMENSIONS_DATE_AGGREGATION.keySet())
						.withMeasures(MEASURES.keySet()));

		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, cubeMetadataStorageSql);
		LogManager<LogItem> logManager = getLogManager(LogItem.class, eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = LogToCubeRunner.create(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		List<LogItem> logItemsForAdvertisersAggregations = asList(
				new LogItem(1, 1, 1, 1, 20, 3, 1, 0.12, 2, 2, EXCLUDE_AFFILIATE, EXCLUDE_SITE),
				new LogItem(1, 2, 2, 2, 100, 5, 0, 0.36, 10, 0, EXCLUDE_AFFILIATE, EXCLUDE_SITE),
				new LogItem(1, 3, 3, 3, 80, 5, 0, 0.60, 1, 8, EXCLUDE_AFFILIATE, EXCLUDE_SITE),
				new LogItem(2, 1, 1, 1, 15, 2, 0, 0.22, 1, 3, EXCLUDE_AFFILIATE, EXCLUDE_SITE),
				new LogItem(3, 1, 1, 1, 30, 5, 2, 0.30, 3, 4, EXCLUDE_AFFILIATE, EXCLUDE_SITE));

		List<LogItem> logItemsForAffiliatesAggregation = asList(
				new LogItem(1, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 10, 3, 1, 0.12, 0, 2, 1, "site3.com"),
				new LogItem(1, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 15, 2, 0, 0.22, 0, 3, 2, "site3.com"),
				new LogItem(1, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 30, 5, 2, 0.30, 0, 4, 2, "site3.com"),
				new LogItem(1, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 100, 5, 0, 0.36, 0, 0, 3, "site2.com"),
				new LogItem(1, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 80, 5, 0, 0.60, 0, 8, 4, "site1.com"),
				new LogItem(2, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 20, 1, 12, 0.8, 0, 3, 4, "site1.com"),
				new LogItem(2, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 30, 2, 13, 0.9, 0, 2, 4, "site1.com"),
				new LogItem(3, EXCLUDE_ADVERTISER, EXCLUDE_CAMPAIGN, EXCLUDE_BANNER, 40, 3, 2, 1.0, 0, 1, 4, "site1.com"));

		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop,
				concat(logItemsForAdvertisersAggregations, logItemsForAffiliatesAggregation).iterator());

		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(IgnoreCompletionCallback.create());
		eventloop.run();

		cube.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		int serverPort = 40000 + new Random().nextInt(100);
		cubeHttpServer = AsyncHttpServer.create(eventloop, ReportingServiceServlet.createRootServlet(cube))
				.withListenPort(serverPort)
				.withAcceptOnce();
		cubeHttpServer.listen();

		httpClient = AsyncHttpClient.create(eventloop)
				.withNoKeepAlive();
		cubeHttpClient = CubeHttpClient.create(eventloop, httpClient, "http://127.0.0.1:" + serverPort)
				.withAttribute("date", LocalDate.class)
				.withAttribute("advertiser", int.class)
				.withAttribute("campaign", int.class)
				.withAttribute("banner", int.class)
				.withAttribute("affiliate", int.class)
				.withAttribute("site", String.class)
				.withAttribute("advertiser.name", String.class)
				.withMeasure("impressions", long.class)
				.withMeasure("clicks", long.class)
				.withMeasure("conversions", long.class)
				.withMeasure("revenue", double.class)
				.withMeasure("errors", long.class)
				.withMeasure("eventCount", int.class)
				.withMeasure("minRevenue", double.class)
				.withMeasure("maxRevenue", double.class)
				.withMeasure("ctr", double.class)
				.withMeasure("uniqueUserIdsCount", int.class)
				.withMeasure("uniqueUserPercent", double.class)
				.withMeasure("errorsPercent", double.class);
	}

	@Test
	public void testQuery() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date")
				.withMeasures("impressions", "clicks", "ctr", "revenue")
				.withOrderingDesc("date")
				.withWhere(and(between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-03"))))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(2, records.size());
		assertEquals(newHashSet("date"), newHashSet(queryResult.getAttributes()));
		assertEquals(newHashSet("impressions", "clicks", "ctr", "revenue"), newHashSet(queryResult.getMeasures()));
		assertEquals(LocalDate.parse("2000-01-03"), records.get(0).get("date"));
		assertEquals(5, (long) records.get(0).get("clicks"));
		assertEquals(65, (long) records.get(0).get("impressions"));
		assertEquals(5.0 / 65.0 * 100.0, (double) records.get(0).get("ctr"), DELTA);
		assertEquals(LocalDate.parse("2000-01-02"), records.get(1).get("date"));
		assertEquals(33, (long) records.get(1).get("clicks"));
		assertEquals(435, (long) records.get(1).get("impressions"));
		assertEquals(33.0 / 435.0 * 100.0, (double) records.get(1).get("ctr"), DELTA);
		assertEquals(2, queryResult.getTotalCount());
		Record totals = queryResult.getTotals();
		assertEquals(38, (long) totals.get("clicks"));
		assertEquals(500, (long) totals.get("impressions"));
		assertEquals(38.0 / 500.0 * 100.0, (double) totals.get("ctr"), DELTA);
		assertEquals(newHashSet("date"), newHashSet(queryResult.getSortedBy()));
	}

	@Test
	public void testQueryWithPredicateLe() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date")
				.withMeasures("impressions", "clicks", "ctr", "revenue")
				.withOrderingDesc("date")
				.withWhere(and(le("date", LocalDate.parse("2000-01-03"))))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(2, records.size());
		assertEquals(newHashSet("date"), newHashSet(queryResult.getAttributes()));
		assertEquals(newHashSet("impressions", "clicks", "ctr", "revenue"), newHashSet(queryResult.getMeasures()));
		assertEquals(LocalDate.parse("2000-01-03"), records.get(0).get("date"));
		assertEquals(5, (long) records.get(0).get("clicks"));
		assertEquals(65, (long) records.get(0).get("impressions"));
		assertEquals(5.0 / 65.0 * 100.0, (double) records.get(0).get("ctr"), DELTA);
		assertEquals(LocalDate.parse("2000-01-02"), records.get(1).get("date"));
		assertEquals(33, (long) records.get(1).get("clicks"));
		assertEquals(435, (long) records.get(1).get("impressions"));
		assertEquals(33.0 / 435.0 * 100.0, (double) records.get(1).get("ctr"), DELTA);
		assertEquals(2, queryResult.getTotalCount());
		Record totals = queryResult.getTotals();
		assertEquals(38, (long) totals.get("clicks"));
		assertEquals(500, (long) totals.get("impressions"));
		assertEquals(38.0 / 500.0 * 100.0, (double) totals.get("ctr"), DELTA);
		assertEquals(newHashSet("date"), newHashSet(queryResult.getSortedBy()));
	}

	@Test
	public void testQueryAffectingAdvertisersAggregation() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date")
				.withMeasures("impressions", "clicks", "ctr", "revenue")
				.withWhere(and(
						eq("banner", 1),
						between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-03")),
						and(notEq("advertiser", EXCLUDE_ADVERTISER), notEq("banner", EXCLUDE_BANNER), notEq("campaign", EXCLUDE_CAMPAIGN))))
				.withOrderings(asc("ctr"))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(2, records.size());
		assertEquals(newHashSet("date"), newHashSet(queryResult.getAttributes()));
		assertEquals(newHashSet("impressions", "clicks", "ctr", "revenue"), newHashSet(queryResult.getMeasures()));
		assertEquals(LocalDate.parse("2000-01-03"), records.get(0).get("date"));
		assertEquals(2, (long) records.get(0).get("clicks"));
		assertEquals(15, (long) records.get(0).get("impressions"));
		assertEquals(2.0 / 15.0 * 100.0, (double) records.get(0).get("ctr"), DELTA);
		assertEquals(LocalDate.parse("2000-01-02"), records.get(1).get("date"));
		assertEquals(3, (long) records.get(1).get("clicks"));
		assertEquals(20, (long) records.get(1).get("impressions"));
		assertEquals(3.0 / 20.0 * 100.0, (double) records.get(1).get("ctr"), DELTA);
		assertEquals(2, queryResult.getTotalCount());
		Record totals = queryResult.getTotals();
		assertEquals(35, (long) totals.get("impressions"));
		assertEquals(5, (long) totals.get("clicks"));
		assertEquals(5.0 / 35.0 * 100.0, (double) totals.get("ctr"), DELTA);
		assertEquals(newHashSet("ctr"), newHashSet(queryResult.getSortedBy()));
	}

	@Test
	public void testImpressionsByDate() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date")
				.withMeasures("impressions")
				.withReportType(DATA);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(3, records.size());

		Record r1 = records.get(0);
		assertEquals(LocalDate.parse("2000-01-02"), r1.get("date"));
		assertEquals(435, (long) r1.get("impressions"));

		Record r2 = records.get(1);
		assertEquals(LocalDate.parse("2000-01-03"), r2.get("date"));
		assertEquals(65, (long) r2.get("impressions"));

		Record r3 = records.get(2);
		assertEquals(LocalDate.parse("2000-01-04"), r3.get("date"));
		assertEquals(70, (long) r3.get("impressions"));
	}

	@Test
	public void testQueryWithNullAttributes() {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date", "advertiser.name", "advertiser")
				.withMeasures("impressions")
				.withOrderings(asc("date"), asc("advertiser.name"))
				.withWhere(and(
						between("date", LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-04")),
						and(notEq("advertiser", EXCLUDE_ADVERTISER), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER))))
				.withHaving(and(
						or(eq("advertiser.name", null), regexp("advertiser.name", ".*f.*")),
						between("date", LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-03"))))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(3, records.size());
		assertEquals(newHashSet("date", "advertiser", "advertiser.name"), newHashSet(queryResult.getAttributes()));
		assertEquals(newHashSet("impressions"), newHashSet(queryResult.getMeasures()));

		assertEquals(LocalDate.parse("2000-01-02"), records.get(0).get("date"));
		assertEquals(2, (int) records.get(0).get("advertiser"));
		assertEquals(null, (String) records.get(0).get("advertiser.name"));
		assertEquals(100, (long) records.get(0).get("impressions"));

		assertEquals(LocalDate.parse("2000-01-02"), records.get(1).get("date"));
		assertEquals(1, (int) records.get(1).get("advertiser"));
		assertEquals("first", (String) records.get(1).get("advertiser.name"));
		assertEquals(20, (long) records.get(1).get("impressions"));

		assertEquals(LocalDate.parse("2000-01-03"), records.get(2).get("date"));
		assertEquals(1, (int) records.get(2).get("advertiser"));
		assertEquals("first", (String) records.get(2).get("advertiser.name"));
		assertEquals(15, (long) records.get(2).get("impressions"));

		Record totals = queryResult.getTotals();
		// totals evaluated before applying having predicate
		assertEquals(245, (long) totals.get("impressions"));
		assertEquals(newHashSet("date", "advertiser.name"), newHashSet(queryResult.getSortedBy()));
	}

	@Test
	public void testQueryWithNullAttributeAndBetweenPredicate() {
		CubeQuery query = CubeQuery.create()
				.withAttributes("advertiser.name")
				.withMeasures("impressions")
				.withWhere(and(notEq("advertiser", EXCLUDE_ADVERTISER), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER)))
				.withHaving(or(between("advertiser.name", "a", "z"), eq("advertiser.name", null)))
				.withReportType(DATA);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(3, records.size());
		assertEquals("first", (String) records.get(0).get("advertiser.name"));
		assertEquals(null, (String) records.get(1).get("advertiser.name"));
		assertEquals("third", (String) records.get(2).get("advertiser.name"));
	}

	@Test
	public void testFilterAttributes() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date", "advertiser.name")
				.withMeasures("impressions")
				.withWhere(and(eq("advertiser", 2), notEq("advertiser", EXCLUDE_ADVERTISER), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER)))
				.withOrderings(asc("advertiser.name"))
				.withHaving(eq("advertiser.name", null));

		final QueryResult queryResult = getQueryResult(query);

		Map<String, Object> filterAttributes = queryResult.getFilterAttributes();
		assertEquals(1, filterAttributes.size());
		assertTrue(filterAttributes.containsKey("advertiser.name"));
		assertNull(filterAttributes.get("advertiser.name"));
	}

	@Test
	public void testRecordsWithFullySpecifiedAttributes() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date", "advertiser.name")
				.withMeasures("impressions")
				.withWhere(and(eq("advertiser", 1), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER)));

		final QueryResult queryResult = getQueryResult(query);

		assertEquals(3, queryResult.getRecords().size());
		assertEquals("first", queryResult.getRecords().get(0).get("advertiser.name"));
		assertEquals("first", queryResult.getRecords().get(1).get("advertiser.name"));
		assertEquals("first", queryResult.getRecords().get(2).get("advertiser.name"));
	}

	@Test
	public void testSearchAndFieldsParameter() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("advertiser.name")
				.withMeasures("clicks")
				.withWhere(and(not(eq("advertiser", EXCLUDE_ADVERTISER)), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER)))
				.withHaving(or(regexp("advertiser.name", ".*s.*"), eq("advertiser.name", null)))
				.withReportType(DATA);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(2, records.size());
		assertEquals(asList("advertiser.name", "clicks"), records.get(0).getScheme().getFields());
		assertEquals(asList("advertiser.name"), queryResult.getAttributes());
		assertEquals(asList("clicks"), queryResult.getMeasures());
		assertEquals("first", records.get(0).get("advertiser.name"));
		assertEquals(null, records.get(1).get("advertiser.name"));
	}

	@Test
	public void testCustomMeasures() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date")
				.withMeasures("eventCount", "minRevenue", "maxRevenue", "uniqueUserIdsCount", "uniqueUserPercent", "clicks")
				.withOrderings(asc("date"), asc("uniqueUserIdsCount"))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult queryResult = getQueryResult(query);

		List<Record> records = queryResult.getRecords();
		assertEquals(newHashSet("eventCount", "minRevenue", "maxRevenue", "uniqueUserIdsCount", "uniqueUserPercent", "clicks"),
				newHashSet(queryResult.getMeasures()));
		assertEquals(3, records.size());

		Record r1 = records.get(0);
		assertEquals(LocalDate.parse("2000-01-02"), r1.get("date"));
		assertEquals(0.12, (double) r1.get("minRevenue"), DELTA);
		assertEquals(0.6, (double) r1.get("maxRevenue"), DELTA);
		assertEquals(8, (int) r1.get("eventCount"));
		assertEquals(4, (int) r1.get("uniqueUserIdsCount"));
		assertEquals(50.0, (double) r1.get("uniqueUserPercent"), DELTA);

		Record r2 = records.get(1);
		assertEquals(LocalDate.parse("2000-01-03"), r2.get("date"));
		assertEquals(0.22, (double) r2.get("minRevenue"), DELTA);
		assertEquals(0.9, (double) r2.get("maxRevenue"), DELTA);
		assertEquals(3, (int) r2.get("eventCount"));
		assertEquals(2, (int) r2.get("uniqueUserIdsCount"));
		assertEquals(2.0 / 3.0 * 100, (double) r2.get("uniqueUserPercent"), DELTA);

		Record r3 = records.get(2);
		assertEquals(LocalDate.parse("2000-01-04"), r3.get("date"));
		assertEquals(0.30, (double) r3.get("minRevenue"), DELTA);
		assertEquals(1.0, (double) r3.get("maxRevenue"), DELTA);
		assertEquals(2, (int) r3.get("eventCount"));
		assertEquals(2, (int) r3.get("uniqueUserIdsCount"));
		assertEquals(100.0, (double) r3.get("uniqueUserPercent"), DELTA);

		Record totals = queryResult.getTotals();
		assertEquals(0.12, (double) totals.get("minRevenue"), DELTA);
		assertEquals(1.0, (double) totals.get("maxRevenue"), DELTA);
		assertEquals(13, (int) totals.get("eventCount"));
		assertEquals(5, (int) totals.get("uniqueUserIdsCount"));
		assertEquals(5 / 13.0 * 100, (double) totals.get("uniqueUserPercent"), DELTA);
		assertEquals(46, (long) totals.get("clicks"));
	}

	@Test
	public void testMetadataOnlyQuery() {
		String[] attributes = {"date", "advertiser", "advertiser.name"};
		CubeQuery onlyMetaQuery = CubeQuery.create()
				.withAttributes(attributes)
				.withWhere(and(
						notEq("advertiser", EXCLUDE_ADVERTISER),
						notEq("banner", EXCLUDE_BANNER),
						notEq("campaign", EXCLUDE_CAMPAIGN)))
				.withMeasures("clicks", "ctr", "conversions")
				.withOrderingDesc("date")
				.withOrderingAsc("advertiser.name")
				.withReportType(ReportType.METADATA);

		final QueryResult metadata = getQueryResult(onlyMetaQuery);

		assertEquals(6, metadata.getRecordScheme().getFields().size());
		assertEquals(0, metadata.getTotalCount());
		assertTrue(metadata.getRecords().isEmpty());
		assertTrue(metadata.getFilterAttributes().isEmpty());
	}

	@Test
	public void testQueryWithInPredicate() {
		String[] attributes = {"advertiser"};
		CubeQuery queryWithPredicateIn = CubeQuery.create()
				.withAttributes(attributes)
				.withWhere(and(in("advertiser", newLinkedHashSet(newArrayList(1, 2))), notEq("advertiser", EXCLUDE_ADVERTISER),
						notEq("banner", EXCLUDE_BANNER),
						notEq("campaign", EXCLUDE_CAMPAIGN)))
				.withMeasures("clicks", "ctr", "conversions")
				.withReportType(DATA_WITH_TOTALS)
				.withHaving(in("advertiser", newArrayList(1, 2)));

		final QueryResult in = getQueryResult(queryWithPredicateIn);

		List<String> expectedRecordFields = newArrayList("advertiser", "clicks", "ctr", "conversions");
		assertEquals(expectedRecordFields.size(), in.getRecordScheme().getFields().size());
		assertEquals(2, in.getTotalCount());

		assertEquals(1, in.getRecords().get(0).get("advertiser"));
		assertEquals(2, in.getRecords().get(1).get("advertiser"));
	}

	@Test
	public void testMetaOnlyQueryHasEmptyMeasuresWhenNoAggregationsFound() {
		CubeQuery queryAffectingNonCompatibleAggregations = CubeQuery.create()
				.withAttributes("date", "advertiser", "affiliate")
				.withMeasures("errors", "errorsPercent")
				.withReportType(ReportType.METADATA);

		final QueryResult metadata = getQueryResult(queryAffectingNonCompatibleAggregations);
		assertEquals(0, metadata.getMeasures().size());
	}

	@Test
	public void testMetaOnlyQueryResultHasCorrectMeasuresWhenSomeAggregationsAreIncompatible() {
		CubeQuery queryAffectingNonCompatibleAggregations = CubeQuery.create()
				.withAttributes("date", "advertiser")
				.withMeasures("impressions", "incompatible_measure", "clicks")
				.withWhere(and(
						notEq("advertiser", EXCLUDE_ADVERTISER),
						notEq("campaign", EXCLUDE_CAMPAIGN),
						notEq("banner", EXCLUDE_BANNER)))
				.withReportType(ReportType.METADATA);

		final QueryResult metadata = getQueryResult(queryAffectingNonCompatibleAggregations);
		List<String> expectedMeasures = newArrayList("impressions", "clicks");
		assertEquals(expectedMeasures, metadata.getMeasures());
	}

	@Test
	public void testDataCorrectlyLoadedIntoAggregations() {
		Aggregation daily = cube.getAggregation("daily");

		int dailyAggregationItemsCount = getAggregationItemsCount(daily);
		assertEquals(6, dailyAggregationItemsCount);

		Aggregation advertisers = cube.getAggregation("advertisers");
		int advertisersAggregationItemsCount = getAggregationItemsCount(advertisers);
		assertEquals(5, advertisersAggregationItemsCount);

		Aggregation affiliates = cube.getAggregation("affiliates");
		int affiliatesAggregationItemsCount = getAggregationItemsCount(affiliates);
		assertEquals(6, affiliatesAggregationItemsCount);
	}

	@Test
	public void testAdvertisersAggregationTotals() {
		CubeQuery queryAdvertisers = CubeQuery.create()
				.withAttributes("date", "advertiser")
				.withMeasures(newArrayList("clicks", "impressions", "revenue", "errors"))
				.withWhere(and(notEq("advertiser", EXCLUDE_ADVERTISER), notEq("campaign", EXCLUDE_CAMPAIGN), notEq("banner", EXCLUDE_BANNER),
						between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-02"))))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult resultByAdvertisers = getQueryResult(queryAdvertisers);

		Record advertisersTotals = resultByAdvertisers.getTotals();
		long advertisersImpressions = (long) advertisersTotals.get("impressions");
		long advertisersClicks = (long) advertisersTotals.get("clicks");
		double advertisersRevenue = (double) advertisersTotals.get("revenue");
		long advertisersErrors = (long) advertisersTotals.get("errors");
		assertEquals(200, advertisersImpressions);
		assertEquals(13, advertisersClicks);
		assertEquals(1.08, advertisersRevenue, Double.MIN_VALUE);
		assertEquals(10, advertisersErrors);
	}

	@Test
	public void testAffiliatesAggregationTotals() {
		CubeQuery queryAffiliates = CubeQuery.create()
				.withAttributes("date", "affiliate")
				.withMeasures(newArrayList("clicks", "impressions", "revenue", "errors"))
				.withWhere(and(notEq("affiliate", 0), notEq("site", EXCLUDE_SITE),
						between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-02"))))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult resultByAffiliates = getQueryResult(queryAffiliates);

		Record affiliatesTotals = resultByAffiliates.getTotals();
		long affiliatesImpressions = (long) affiliatesTotals.get("impressions");
		long affiliatesClicks = (long) affiliatesTotals.get("clicks");
		double affiliatesRevenue = (double) affiliatesTotals.get("revenue");
		long affiliatesErrors = (long) affiliatesTotals.get("errors");
		assertEquals(235, affiliatesImpressions);
		assertEquals(20, affiliatesClicks);
		assertEquals(1.60, affiliatesRevenue, Double.MIN_VALUE);
		assertEquals(17, affiliatesErrors);
	}

	@Test
	public void testDailyAggregationTotals() {
		CubeQuery queryDate = CubeQuery.create()
				.withAttributes("date")
				.withMeasures(newArrayList("clicks", "impressions", "revenue", "errors"))
				.withWhere(between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-02")))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult resultByDate = getQueryResult(queryDate);

		Record dailyTotals = resultByDate.getTotals();
		long dailyImpressions = (long) dailyTotals.get("impressions");
		long dailyClicks = (long) dailyTotals.get("clicks");
		double dailyRevenue = (double) dailyTotals.get("revenue");
		long dailyErrors = (long) dailyTotals.get("errors");
		assertEquals(435, dailyImpressions);
		assertEquals(33, dailyClicks);
		assertEquals(2.68, dailyRevenue, DELTA);
		assertEquals(27, dailyErrors);
	}

	@Test
	public void testResultContainsTotals_whenDataWithTotalsRequested() {
		ArrayList<String> measures = newArrayList("clicks", "impressions", "revenue", "errors");
		ArrayList<String> requestMeasures = newArrayList(measures);
		requestMeasures.add(3, "nonexistentMeasure");
		List<String> dateDimension = singletonList("date");
		CubeQuery queryDate = CubeQuery.create()
				.withAttributes(dateDimension)
				.withMeasures(requestMeasures)
				.withWhere(between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-02")))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult resultByDate = getQueryResult(queryDate);

		assertEquals(dateDimension, resultByDate.getAttributes());
		assertEquals(measures, resultByDate.getMeasures());

		Record dailyTotals = resultByDate.getTotals();
		long dailyImpressions = (long) dailyTotals.get("impressions");
		long dailyClicks = (long) dailyTotals.get("clicks");
		double dailyRevenue = (double) dailyTotals.get("revenue");
		long dailyErrors = (long) dailyTotals.get("errors");
		assertEquals(435, dailyImpressions);
		assertEquals(33, dailyClicks);
		assertEquals(2.68, dailyRevenue, DELTA);
		assertEquals(27, dailyErrors);
	}

	@Test
	public void testResultContainsTotalsAndMetadata_whenTotalsAndMetadataRequested() {
		ArrayList<String> measures = newArrayList("clicks", "impressions", "revenue", "errors");
		ArrayList<String> requestMeasures = newArrayList(measures);
		requestMeasures.add("unexpected");
		CubeQuery queryDate = CubeQuery.create()
				.withAttributes("date")
				.withMeasures(requestMeasures)
				.withWhere(between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-02")))
				.withReportType(DATA_WITH_TOTALS);

		final QueryResult resultByDate = getQueryResult(queryDate);

		assertTrue(resultByDate.getAttributes().size() == 1);
		assertEquals("date", resultByDate.getAttributes().get(0));
		assertTrue(resultByDate.getMeasures().equals(measures));
		Record dailyTotals = resultByDate.getTotals();
		long dailyImpressions = (long) dailyTotals.get("impressions");
		long dailyClicks = (long) dailyTotals.get("clicks");
		double dailyRevenue = (double) dailyTotals.get("revenue");
		long dailyErrors = (long) dailyTotals.get("errors");
		assertEquals(435, dailyImpressions);
		assertEquals(33, dailyClicks);
		assertEquals(2.68, dailyRevenue, DELTA);
		assertEquals(27, dailyErrors);
	}

	private static int getAggregationItemsCount(Aggregation aggregation) {
		int count = 0;
		for (Map.Entry<Long, AggregationChunk> chunk : aggregation.getMetadata().getChunks().entrySet()) {
			count += chunk.getValue().getCount();
		}
		return count;
	}

	private QueryResult getQueryResult(CubeQuery query) {
		final QueryResult[] queryResult = new QueryResult[1];
		cubeHttpClient.query(query, new AssertingResultCallback<QueryResult>() {
			@Override
			protected void onResult(QueryResult result) {
				queryResult[0] = result;
			}
		});
		eventloop.run();
		return queryResult[0];
	}
}
