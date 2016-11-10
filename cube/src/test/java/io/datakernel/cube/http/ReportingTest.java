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
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.LocalFsChunkStorage;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.*;
import static io.datakernel.aggregation.AggregationPredicates.*;
import static io.datakernel.aggregation.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation.measure.Measures.*;
import static io.datakernel.cube.ComputedMeasures.*;
import static io.datakernel.cube.Cube.AggregationScheme.id;
import static io.datakernel.cube.CubeQuery.Ordering.asc;
import static io.datakernel.cube.CubeQuery.Ordering.desc;
import static io.datakernel.cube.CubeTestUtils.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReportingTest {
	private static final Logger logger = LoggerFactory.getLogger(ReportingTest.class);
	public static final double DELTA = 1E-3;

	private Eventloop eventloop;
	private AsyncHttpServer server;
	private AsyncHttpClient httpClient;
	private CubeHttpClient cubeHttpClient;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String DATABASE_PROPERTIES_PATH = "test.properties";
	private static final SQLDialect DATABASE_DIALECT = SQLDialect.MYSQL;
	private static final String LOG_PARTITION_NAME = "partitionA";
	private static final List<String> LOG_PARTITIONS = singletonList(LOG_PARTITION_NAME);
	private static final String LOG_NAME = "testlog";

	private static final int SERVER_PORT = 50001;
	private static final int TIMEOUT = 1000;

	private static final Map<String, FieldType> DIMENSIONS = ImmutableMap.<String, FieldType>builder()
			.put("date", ofLocalDate(LocalDate.parse("2000-01-01")))
			.put("advertiser", ofInt())
			.put("campaign", ofInt())
			.put("banner", ofInt())
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
			.build();

	private static final Map<String, String> OUTPUT_TO_INPUT_FIELDS;

	static {
		OUTPUT_TO_INPUT_FIELDS = new HashMap<>();
		OUTPUT_TO_INPUT_FIELDS.put("eventCount", null);
		OUTPUT_TO_INPUT_FIELDS.put("minRevenue", "revenue");
		OUTPUT_TO_INPUT_FIELDS.put("maxRevenue", "revenue");
		OUTPUT_TO_INPUT_FIELDS.put("uniqueUserIdsCount", "userId");
	}

	private static class AdvertiserResolver extends AbstractAttributeResolver<Integer, String> {
		@Override
		public Class<?>[] getKeyTypes() {
			return new Class[]{Integer.class};
		}

		@Override
		protected Integer toKey(Object[] keyArray) {
			return (Integer) keyArray[0];
		}

		@Override
		public Class<?>[] getAttributeTypes() {
			return new Class[]{String.class};
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
					return "second";
				case 3:
					return "third";
				default:
					return null;
			}
		}
	}

	public static class LogItem {
		/* Dimensions */
		@Serialize(order = 0)
		public int date;

		@Serialize(order = 1)
		public int advertiser;

		@Serialize(order = 2)
		public int campaign;

		@Serialize(order = 3)
		public int banner;

		/* Measures */
		@Serialize(order = 4)
		public long impressions;

		@Serialize(order = 5)
		public long clicks;

		@Serialize(order = 6)
		public long conversions;

		@Serialize(order = 7)
		public double revenue;

		@Serialize(order = 8)
		public int userId;

		public LogItem() {
		}

		public LogItem(int date, int advertiser, int campaign, int banner,
		               long impressions, long clicks, long conversions, double revenue, int userId) {
			this.date = date;
			this.advertiser = advertiser;
			this.campaign = campaign;
			this.banner = banner;
			this.impressions = impressions;
			this.clicks = clicks;
			this.conversions = conversions;
			this.revenue = revenue;
			this.userId = userId;
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

		private StreamDataReceiver<LogItem> logItemAggregator;

		@Override
		protected void addOutputs() {
			logItemAggregator = addOutput(LogItem.class, newArrayList(DIMENSIONS.keySet()),
					newArrayList(MEASURES.keySet()), OUTPUT_TO_INPUT_FIELDS);
		}

		@Override
		protected void processItem(LogItem item) {
			logItemAggregator.onData(item);
		}
	}

	@Before
	public void setUp() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Configuration jooqConfiguration = getJooqConfiguration(DATABASE_PROPERTIES_PATH, DATABASE_DIALECT);
		AggregationChunkStorage aggregationChunkStorage =
				LocalFsChunkStorage.create(eventloop, executor, aggregationsDir);
		CubeMetadataStorageSql cubeMetadataStorageSql =
				CubeMetadataStorageSql.create(eventloop, executor, jooqConfiguration, "processId");

		Cube cube = Cube.create(eventloop, executor, classLoader, cubeMetadataStorageSql, aggregationChunkStorage)
				.withDimensions(DIMENSIONS)
				.withMeasures(MEASURES)
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign")
				.withAttribute("advertiser.name", new AdvertiserResolver())
				.withComputedMeasure("ctr", percent(measure("clicks"), measure("impressions")))
				.withComputedMeasure("uniqueUserPercent", percent(div(measure("uniqueUserIdsCount"), measure("eventCount"))))
				.withAggregation(id("detailed")
						.withDimensions(DIMENSIONS.keySet())
						.withMeasures(difference(MEASURES.keySet(), singleton("revenue"))));

		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, cubeMetadataStorageSql);
		LogManager<LogItem> logManager = getLogManager(LogItem.class, eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = LogToCubeRunner.create(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		List<LogItem> logItems = asList(
				new LogItem(0, 1, 1, 1, 10, 1, 1, 0.14, 1),
				new LogItem(1, 1, 1, 1, 20, 3, 1, 0.12, 2),
				new LogItem(2, 1, 1, 1, 15, 2, 0, 0.22, 1),
				new LogItem(3, 1, 1, 1, 30, 5, 2, 0.30, 3),
				new LogItem(1, 2, 2, 2, 100, 5, 0, 0.36, 10),
				new LogItem(1, 3, 3, 3, 80, 5, 0, 0.60, 1));
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems =
				new StreamProducers.OfIterator<>(eventloop, logItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(IgnoreCompletionCallback.create());
		eventloop.run();

		cube.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		server = AsyncHttpServer.create(eventloop, ReportingServiceServlet.createRootServlet(eventloop, cube))
				.withListenPort(SERVER_PORT)
				.withAcceptOnce();
		server.listen();

		httpClient = AsyncHttpClient.create(eventloop)
				.withNoKeepAlive();
		cubeHttpClient = CubeHttpClient.create(eventloop, "http://127.0.0.1:" + SERVER_PORT, httpClient, TIMEOUT)
				.withAttribute("date", LocalDate.class)
				.withAttribute("advertiser", int.class)
				.withAttribute("campaign", int.class)
				.withAttribute("banner", int.class)
				.withAttribute("advertiser.name", String.class)
				.withMeasure("impressions", long.class)
				.withMeasure("clicks", long.class)
				.withMeasure("conversions", long.class)
				.withMeasure("revenue", double.class)
				.withMeasure("eventCount", int.class)
				.withMeasure("minRevenue", double.class)
				.withMeasure("maxRevenue", double.class)
				.withMeasure("ctr", double.class)
				.withMeasure("uniqueUserIdsCount", int.class)
				.withMeasure("uniqueUserPercent", double.class);
	}

	@Test
	public void testQuery() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date", "campaign")
				.withMeasures("impressions", "clicks", "ctr", "revenue")
				.withPredicate(and(
						eq("banner", 1),
						between("date", LocalDate.parse("2000-01-02"), LocalDate.parse("2000-01-03"))))
				.withOrderings(asc("campaign"), asc("ctr"), desc("banner"));

		final QueryResult[] queryResult = new QueryResult[1];
		cubeHttpClient.query(query, new AssertingResultCallback<QueryResult>() {
			@Override
			protected void onResult(QueryResult result) {
				queryResult[0] = result;
			}
		});

		eventloop.run();

		List<Record> records = queryResult[0].getRecords();
		assertEquals(2, records.size());
		assertEquals(newHashSet("date", "advertiser", "campaign"), newHashSet(queryResult[0].getAttributes()));
		assertEquals(newHashSet("impressions", "clicks", "ctr"), newHashSet(queryResult[0].getMeasures()));
		assertEquals(LocalDate.parse("2000-01-03"), records.get(0).get("date"));
		assertEquals(1, (int) records.get(0).get("advertiser"));
		assertEquals(2, (long) records.get(0).get("clicks"));
		assertEquals(15, (long) records.get(0).get("impressions"));
		assertEquals(2.0 / 15.0 * 100.0, (double) records.get(0).get("ctr"), DELTA);
		assertEquals(LocalDate.parse("2000-01-02"), records.get(1).get("date"));
		assertEquals(1, (int) records.get(1).get("advertiser"));
		assertEquals(3, (long) records.get(1).get("clicks"));
		assertEquals(20, (long) records.get(1).get("impressions"));
		assertEquals(3.0 / 20.0 * 100.0, (double) records.get(1).get("ctr"), DELTA);
		assertEquals(2, queryResult[0].getTotalCount());
		Record totals = queryResult[0].getTotals();
		assertEquals(35, (long) totals.get("impressions"));
		assertEquals(5, (long) totals.get("clicks"));
		assertEquals(5.0 / 35.0 * 100.0, (double) totals.get("ctr"), DELTA);
		assertEquals(newHashSet("campaign", "ctr"), newHashSet(queryResult[0].getSortedBy()));
		assertTrue(queryResult[0].getDrilldowns().isEmpty());
//		assertTrue(queryResult[0].getAttributes().isEmpty());
	}

	@Test
	public void testPaginationAndDrillDowns() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date")
				.withMeasures("impressions", "revenue", "ctr")
				.withLimit(1)
				.withOffset(2);

		final QueryResult[] queryResult = new QueryResult[1];
		cubeHttpClient.query(query, new AssertingResultCallback<QueryResult>() {
			@Override
			protected void onResult(QueryResult result) {
				queryResult[0] = result;
			}
		});

		eventloop.run();

		Set<QueryResult.Drilldown> drilldowns = newLinkedHashSet();
		drilldowns.add(QueryResult.Drilldown.create(asList("advertiser"), singleton("impressions")));
		drilldowns.add(QueryResult.Drilldown.create(asList("advertiser", "campaign"), singleton("impressions")));
		drilldowns.add(QueryResult.Drilldown.create(asList("advertiser", "campaign", "banner"), singleton("impressions")));
		assertEquals(drilldowns, newHashSet(queryResult[0].getDrilldowns()));

		List<Record> records = queryResult[0].getRecords();
		assertEquals(1, records.size());
		assertEquals(3, records.get(0).getScheme().getFields().size());
		assertEquals(LocalDate.parse("2000-01-03"), records.get(0).get("date"));
		assertEquals(15, (long) records.get(0).get("impressions"));
		assertEquals(2.0 / 15.0 * 100.0, (double) records.get(0).get("ctr"), DELTA);
		assertEquals(4, queryResult[0].getTotalCount());

	}

	@Test
	public void testFilterAttributes() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("date", "advertiser.name")
				.withMeasures("impressions")
				.withPredicate(eq("advertiser", 1))
				.withOrderings(asc("advertiser.name"))
				.withHaving(or(eq("advertiser.name", "first"), eq("impressions", 10)))
				.withLimit(3);

		final QueryResult[] queryResult = new QueryResult[1];
		cubeHttpClient.query(query, new AssertingResultCallback<QueryResult>() {
			@Override
			protected void onResult(QueryResult result) {
				queryResult[0] = result;
			}
		});

		eventloop.run();

		Map<String, Object> filterAttributes = queryResult[0].getFilterAttributes();
		assertEquals(1, filterAttributes.size());
		assertEquals("first", filterAttributes.get("advertiser.name"));
	}

	@Test
	public void testSearchAndFieldsParameter() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("advertiser.name")
				.withMeasures("clicks")
				.withHaving(regexp("advertiser.name", ".*s.*"));

		final QueryResult[] queryResult = new QueryResult[1];
		cubeHttpClient.query(query, new AssertingResultCallback<QueryResult>() {
			@Override
			protected void onResult(QueryResult result) {
				queryResult[0] = result;
			}
		});

		eventloop.run();

		List<Record> records = queryResult[0].getRecords();
		assertEquals(2, records.size());
		assertEquals(asList("advertiser", "advertiser.name", "clicks"), records.get(0).getScheme().getFields());
		assertEquals(asList("advertiser", "advertiser.name"), queryResult[0].getAttributes());
		assertEquals(asList("clicks"), queryResult[0].getMeasures());
		assertEquals(1, (int) records.get(0).get("advertiser"));
		assertEquals("first", records.get(0).get("advertiser.name"));
		assertEquals(2, (int) records.get(1).get("advertiser"));
		assertEquals("second", records.get(1).get("advertiser.name"));
	}

	@Test
	public void testCustomMeasures() throws Exception {
		CubeQuery query = CubeQuery.create()
				.withAttributes("advertiser")
				.withMeasures("eventCount", "minRevenue", "maxRevenue", "uniqueUserIdsCount", "uniqueUserPercent")
				.withOrderings(asc("uniqueUserIdsCount"), asc("advertiser"));

		final QueryResult[] queryResult = new QueryResult[1];
		cubeHttpClient.query(query, new AssertingResultCallback<QueryResult>() {
			@Override
			protected void onResult(QueryResult result) {
				queryResult[0] = result;
			}
		});

		eventloop.run();

		List<Record> records = queryResult[0].getRecords();
		assertEquals(newHashSet("eventCount", "minRevenue", "maxRevenue", "uniqueUserIdsCount", "uniqueUserPercent"),
				newHashSet(queryResult[0].getMeasures()));
		assertEquals(asList("uniqueUserIdsCount", "advertiser"), queryResult[0].getSortedBy());
		assertEquals(3, records.size());

		Record r1 = records.get(0);
		assertEquals(2, (int) r1.get("advertiser"));
		assertEquals(0.36, (double) r1.get("minRevenue"), DELTA);
		assertEquals(0.36, (double) r1.get("maxRevenue"), DELTA);
		assertEquals(1, (int) r1.get("eventCount"));
		assertEquals(1, (int) r1.get("uniqueUserIdsCount"));
		assertEquals(100, (double) r1.get("uniqueUserPercent"), DELTA);

		Record r2 = records.get(1);
		assertEquals(3, (int) r2.get("advertiser"));
		assertEquals(0.60, (double) r2.get("minRevenue"), DELTA);
		assertEquals(0.60, (double) r2.get("maxRevenue"), DELTA);
		assertEquals(1, (int) r2.get("eventCount"));
		assertEquals(1, (int) r2.get("uniqueUserIdsCount"));
		assertEquals(100, (double) r2.get("uniqueUserPercent"), DELTA);

		Record r3 = records.get(2);
		assertEquals(1, (int) r3.get("advertiser"));
		assertEquals(0.12, (double) r3.get("minRevenue"), DELTA);
		assertEquals(0.30, (double) r3.get("maxRevenue"), DELTA);
		assertEquals(4, (int) r3.get("eventCount"));
		assertEquals(3, (int) r3.get("uniqueUserIdsCount"));
		assertEquals(3.0 / 4.0 * 100.0, (double) r3.get("uniqueUserPercent"), DELTA);

		Record totals = queryResult[0].getTotals();
		assertEquals(0.12, (double) totals.get("minRevenue"), DELTA);
		assertEquals(0.60, (double) totals.get("maxRevenue"), DELTA);
		assertEquals(6, (int) totals.get("eventCount"));
		assertEquals(4, (int) totals.get("uniqueUserIdsCount"));
		assertEquals(4.0 / 6.0 * 100.0, (double) totals.get("uniqueUserPercent"), DELTA);
	}

}
