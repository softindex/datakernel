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

package io.datakernel.cube.api;

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.HyperLogLog;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.RunnableWithException;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.*;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpUtils;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogToCubeMetadataStorage;
import io.datakernel.logfs.LogToCubeRunner;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducers;
import io.datakernel.util.Function;
import org.joda.time.LocalDate;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation_db.keytype.KeyTypes.dateKey;
import static io.datakernel.aggregation_db.keytype.KeyTypes.intKey;
import static io.datakernel.codegen.Expressions.call;
import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.cube.CubeTestUtils.*;
import static io.datakernel.cube.api.ReportingDSL.*;
import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore("Requires DB access to run")
public class ReportingTest {
	private static final Logger logger = LoggerFactory.getLogger(ReportingTest.class);

	private Eventloop eventloop;
	private Eventloop clientEventloop;
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

	private static final Map<String, KeyType> DIMENSIONS = ImmutableMap.<String, KeyType>builder()
			.put("date", dateKey(LocalDate.now()))
			.put("advertiser", intKey())
			.put("campaign", intKey())
			.put("banner", intKey())
			.build();

	private static final Map<String, FieldType> MEASURES = ImmutableMap.<String, FieldType>builder()
			.put("impressions", longSum())
			.put("clicks", longSum())
			.put("conversions", longSum())
			.put("revenue", doubleSum())
			.put("eventCount", intCount())
			.put("minRevenue", doubleMin())
			.put("maxRevenue", doubleMax())
			.put("uniqueUserIdsCount", hyperLogLog(1024))
			.build();

	private static final Map<String, ReportingDSLExpression> COMPUTED_MEASURES = ImmutableMap.<String, ReportingDSLExpression>builder()
			.put("ctr", percent(divide("clicks", "impressions")))
			.put("uniqueUserPercent", percent(divide(preprocess("uniqueUserIdsCount", new Function<Expression, Expression>() {
				@Override
				public Expression apply(Expression input) {
					return call(cast(input, HyperLogLog.class), "estimate");
				}
			}), "eventCount")))
			.build();

	private static final Map<String, String> CHILD_PARENT_RELATIONSHIPS = ImmutableMap.<String, String>builder()
			.put("campaign", "advertiser")
			.put("banner", "campaign")
			.build();

	private static final Map<String, String> OUTPUT_TO_INPUT_FIELDS;

	static {
		OUTPUT_TO_INPUT_FIELDS = new HashMap<>();
		OUTPUT_TO_INPUT_FIELDS.put("eventCount", null);
		OUTPUT_TO_INPUT_FIELDS.put("minRevenue", "revenue");
		OUTPUT_TO_INPUT_FIELDS.put("maxRevenue", "revenue");
		OUTPUT_TO_INPUT_FIELDS.put("uniqueUserIdsCount", "userId");
	}

	private static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader, DIMENSIONS, MEASURES);
	}

	private static Cube getCube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                            CubeMetadataStorage cubeMetadataStorage,
	                            AggregationChunkStorage aggregationChunkStorage,
	                            AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, aggregationChunkStorage,
				cubeStructure, Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY, Aggregation.DEFAULT_SORTER_BLOCK_SIZE,
				Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE);
		Set<String> measures = newHashSet(MEASURES.keySet());
		measures.remove("revenue");
		cube.addAggregation("detailed", new AggregationMetadata(newArrayList(DIMENSIONS.keySet()),
				newArrayList(measures)));
		cube.setChildParentRelationships(CHILD_PARENT_RELATIONSHIPS);
		return cube;
	}

	private static ReportingConfiguration getReportingConfiguration() {
		return new ReportingConfiguration()
				.addResolvedAttributeForKey("advertiserName", singletonList("advertiser"), String.class, new AdvertiserResolver())
				.setComputedMeasures(COMPUTED_MEASURES);
	}

	private static class AdvertiserResolver implements AttributeResolver {
		@Override
		public Map<PrimaryKey, Object[]> resolve(Set<PrimaryKey> keys, List<String> attributes) {
			Map<PrimaryKey, Object[]> result = newHashMap();

			for (PrimaryKey key : keys) {
				String s = key.get(0).toString();

				switch (s) {
					case "1":
						result.put(key, new Object[]{"first"});
						break;
					case "2":
						result.put(key, new Object[]{"second"});
						break;
					case "3":
						result.put(key, new Object[]{"third"});
						break;
				}
			}

			return result;
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

		DefiningClassLoader classLoader = new DefiningClassLoader();
		eventloop = new Eventloop();
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();
		AggregationStructure structure = getStructure(classLoader);

		ReportingConfiguration reportingConfiguration = getReportingConfiguration();
		Configuration jooqConfiguration = getJooqConfiguration(DATABASE_PROPERTIES_PATH, DATABASE_DIALECT);
		AggregationChunkStorage aggregationChunkStorage =
				getAggregationChunkStorage(eventloop, executor, structure, aggregationsDir);
		CubeMetadataStorageSql cubeMetadataStorageSql =
				new CubeMetadataStorageSql(eventloop, executor, jooqConfiguration, "processId");
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, cubeMetadataStorageSql);
		Cube cube = getCube(eventloop, executor, classLoader, cubeMetadataStorageSql, aggregationChunkStorage, structure);
		LogManager<LogItem> logManager = getLogManager(LogItem.class, eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);
		cube.setReportingConfiguration(reportingConfiguration);

		List<LogItem> logItems = asList(new LogItem(0, 1, 1, 1, 10, 1, 1, 0.14, 1),
				new LogItem(1, 1, 1, 1, 20, 3, 1, 0.12, 2), new LogItem(2, 1, 1, 1, 15, 2, 0, 0.22, 1),
				new LogItem(3, 1, 1, 1, 30, 5, 2, 0.30, 3), new LogItem(1, 2, 2, 2, 100, 5, 0, 0.36, 10),
				new LogItem(1, 3, 3, 3, 80, 5, 0, 0.60, 1));
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems =
				new StreamProducers.OfIterator<>(eventloop, logItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		server = CubeHttpServer.createServer(cube, eventloop, classLoader, SERVER_PORT);
		final CompletionCallbackFuture serverStartFuture = new CompletionCallbackFuture();
		eventloop.execute(new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				server.listen();
				serverStartFuture.onComplete();
			}
		});
		new Thread(eventloop).start();
		serverStartFuture.await();

		clientEventloop = new Eventloop();
		NativeDnsResolver dnsClient = new NativeDnsResolver(clientEventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, TIMEOUT,
				HttpUtils.inetAddress("8.8.8.8"));
		httpClient = new AsyncHttpClient(clientEventloop, dnsClient);
		cubeHttpClient = new CubeHttpClient("http://127.0.0.1:" + SERVER_PORT, httpClient, TIMEOUT, structure, reportingConfiguration);
	}

	@Test
	public void testQuery() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.dimensions("date", "campaign")
				.measures("impressions", "clicks", "ctr", "revenue")
				.filters(new AggregationQuery.Predicates()
						.eq("banner", 1)
						.between("date", 1, 2))
				.sort(CubeQuery.Ordering.asc("campaign"), CubeQuery.Ordering.asc("ctr"),
						CubeQuery.Ordering.desc("banner"))
				.metadataFields("dimensions", "measures", "attributes", "drillDowns", "sortedBy");

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		List<Map<String, Object>> records = queryResult[0].getRecords();
		assertEquals(2, records.size());
		assertEquals(6, records.get(0).size());
		assertEquals(2, ((Number) records.get(0).get("date")).intValue());
		assertEquals(1, ((Number) records.get(0).get("advertiser")).intValue());
		assertEquals(2, ((Number) records.get(0).get("clicks")).intValue());
		assertEquals(15, ((Number) records.get(0).get("impressions")).intValue());
		assertEquals(2.0 / 15.0 * 100.0, ((Number) records.get(0).get("ctr")).doubleValue(), 1E-3);
		assertEquals(1, ((Number) records.get(1).get("date")).intValue());
		assertEquals(1, ((Number) records.get(1).get("advertiser")).intValue());
		assertEquals(3, ((Number) records.get(1).get("clicks")).intValue());
		assertEquals(20, ((Number) records.get(1).get("impressions")).intValue());
		assertEquals(3.0 / 20.0 * 100.0, ((Number) records.get(1).get("ctr")).doubleValue(), 1E-3);
		assertEquals(2, queryResult[0].getCount());
		assertEquals(newHashSet("impressions", "clicks", "ctr"), newHashSet(queryResult[0].getMeasures()));
		assertEquals(newHashSet("date", "advertiser", "campaign"), newHashSet(queryResult[0].getDimensions()));
		assertEquals(newHashSet("campaign", "ctr"), newHashSet(queryResult[0].getSortedBy()));
		assertTrue(queryResult[0].getDrillDowns().isEmpty());
		assertTrue(queryResult[0].getAttributes().isEmpty());
		Map<String, Object> totals = queryResult[0].getTotals();
		assertEquals(35, ((Number) totals.get("impressions")).intValue());
		assertEquals(5, ((Number) totals.get("clicks")).intValue());
		assertEquals(5.0 / 35.0 * 100.0, ((Number) totals.get("ctr")).doubleValue(), 1E-3);
	}

	@Test
	public void testPaginationAndDrillDowns() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.dimensions("date")
				.measures("impressions", "revenue")
				.limit(1)
				.offset(2)
				.metadataFields("drillDowns");

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		List<Map<String, Object>> records = queryResult[0].getRecords();
		assertEquals(1, records.size());
		assertEquals(2, records.get(0).size());
		assertEquals(2, ((Number) records.get(0).get("date")).intValue());
		assertEquals(15, ((Number) records.get(0).get("impressions")).intValue());
		assertEquals(4, queryResult[0].getCount());

		Set<DrillDown> drillDowns = newHashSet();
		drillDowns.add(new DrillDown(singletonList("advertiser"), singleton("impressions")));
		drillDowns.add(new DrillDown(asList("advertiser", "campaign"), singleton("impressions")));
		drillDowns.add(new DrillDown(asList("advertiser", "campaign", "banner"), singleton("impressions")));
		assertEquals(drillDowns, queryResult[0].getDrillDowns());
	}

	@Test
	public void testFilterAttributes() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.dimensions("date")
				.attributes("advertiserName")
				.measures("impressions")
				.limit(0)
				.filters(new AggregationQuery.Predicates().eq("advertiser", 1))
				.metadataFields("filterAttributes");

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		Map<String, Object> filterAttributes = queryResult[0].getFilterAttributes();
		assertEquals(1, filterAttributes.size());
		assertEquals("first", filterAttributes.get("advertiserName"));
	}

	@Test
	public void testSearchAndFieldsParameter() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.attributes("advertiserName")
				.measures("clicks")
				.fields("advertiser", "advertiserName")
				.search("s")
				.metadataFields("measures");

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		List<Map<String, Object>> records = queryResult[0].getRecords();
		assertEquals(2, records.size());
		assertEquals(2, records.get(0).size());
		assertEquals(1, ((Number) records.get(0).get("advertiser")).intValue());
		assertEquals("first", records.get(0).get("advertiserName"));
		assertEquals(2, ((Number) records.get(1).get("advertiser")).intValue());
		assertEquals("second", records.get(1).get("advertiserName"));
		assertEquals(singletonList("clicks"), queryResult[0].getMeasures());
	}

	@Test
	public void testCustomMeasures() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.dimensions("advertiser")
				.measures("eventCount", "minRevenue", "maxRevenue", "uniqueUserIdsCount", "uniqueUserPercent")
				.sort(CubeQuery.Ordering.asc("uniqueUserIdsCount"), CubeQuery.Ordering.asc("advertiser"));

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		List<Map<String, Object>> records = queryResult[0].getRecords();
		assertEquals(newHashSet("eventCount", "minRevenue", "maxRevenue", "uniqueUserIdsCount", "uniqueUserPercent"),
				newHashSet(queryResult[0].getMeasures()));
		assertEquals(asList("uniqueUserIdsCount", "advertiser"), queryResult[0].getSortedBy());
		assertEquals(3, records.size());

		Map<String, Object> r1 = records.get(0);
		assertEquals(2, ((Number) r1.get("advertiser")).intValue());
		assertEquals(0.36, ((Number) r1.get("minRevenue")).doubleValue(), 1E-3);
		assertEquals(0.36, ((Number) r1.get("maxRevenue")).doubleValue(), 1E-3);
		assertEquals(1, ((Number) r1.get("eventCount")).intValue());
		assertEquals(1, ((Number) r1.get("uniqueUserIdsCount")).intValue());
		assertEquals(100, ((Number) r1.get("uniqueUserPercent")).intValue());

		Map<String, Object> r2 = records.get(1);
		assertEquals(3, ((Number) r2.get("advertiser")).intValue());
		assertEquals(0.60, ((Number) r2.get("minRevenue")).doubleValue(), 1E-3);
		assertEquals(0.60, ((Number) r2.get("maxRevenue")).doubleValue(), 1E-3);
		assertEquals(1, ((Number) r2.get("eventCount")).intValue());
		assertEquals(1, ((Number) r2.get("uniqueUserIdsCount")).intValue());
		assertEquals(100, ((Number) r2.get("uniqueUserPercent")).intValue());

		Map<String, Object> r3 = records.get(2);
		assertEquals(1, ((Number) r3.get("advertiser")).intValue());
		assertEquals(0.12, ((Number) r3.get("minRevenue")).doubleValue(), 1E-3);
		assertEquals(0.30, ((Number) r3.get("maxRevenue")).doubleValue(), 1E-3);
		assertEquals(4, ((Number) r3.get("eventCount")).intValue());
		assertEquals(3, ((Number) r3.get("uniqueUserIdsCount")).intValue());
		assertEquals(3.0 / 4.0 * 100.0, ((Number) r3.get("uniqueUserPercent")).doubleValue());

		Map<String, Object> totals = queryResult[0].getTotals();
		assertEquals(0.12, ((Number) totals.get("minRevenue")).doubleValue(), 1E-3);
		assertEquals(0.60, ((Number) totals.get("maxRevenue")).doubleValue(), 1E-3);
		assertEquals(6, ((Number) totals.get("eventCount")).intValue());
		assertEquals(4, ((Number) totals.get("uniqueUserIdsCount")).intValue());
		assertEquals(4.0 / 6.0 * 100.0, ((Number) totals.get("uniqueUserPercent")).doubleValue(), 1E-3);
	}

	@After
	public void tearDown() throws Exception {
		final CompletionCallbackFuture serverStopFuture = new CompletionCallbackFuture();
		eventloop.execute(new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				server.close();
				serverStopFuture.onComplete();
			}
		});
		serverStopFuture.await();
	}

	private static void startBlocking(EventloopService service) throws ExecutionException, InterruptedException {
		CompletionCallbackFuture future = new CompletionCallbackFuture();
		service.start(future);

		try {
			future.await();
		} catch (Exception e) {
			logger.error("Service {} start exception", e);
		}
	}

	private static void stopBlocking(EventloopService service) {
		CompletionCallbackFuture future = new CompletionCallbackFuture();
		service.stop(future);

		try {
			future.await();
		} catch (Exception e) {
			logger.error("Service {} stop exception", e);
		}
	}
}
