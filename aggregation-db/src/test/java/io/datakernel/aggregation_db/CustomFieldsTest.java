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

package io.datakernel.aggregation_db;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.HyperLogLog;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.aggregation_db.fieldtype.FieldTypes.*;
import static io.datakernel.aggregation_db.keytype.KeyTypes.intKey;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;

public class CustomFieldsTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static class EventRecord {
		// dimensions
		public int siteId;

		// measures
		public double revenue;
		public long userId;

		public EventRecord(int siteId, double revenue, long userId) {
			this.siteId = siteId;
			this.revenue = revenue;
			this.userId = userId;
		}
	}

	public static class QueryResult {
		public int siteId;

		public long eventCount;
		public double sumRevenue;
		public double minRevenue;
		public double maxRevenue;
		public List<Long> userIds;
		public Set<Long> uniqueUserIds;
		public HyperLogLog estimatedUniqueUserIdCount;

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("siteId", siteId)
					.add("eventCount", eventCount)
					.add("sumRevenue", sumRevenue)
					.add("minRevenue", minRevenue)
					.add("maxRevenue", maxRevenue)
					.add("userIds", userIds)
					.add("uniqueUserIds", uniqueUserIds)
					.add("estimatedUniqueUserIdCount", estimatedUniqueUserIdCount)
					.toString();
		}
	}

	public static final List<String> KEYS = singletonList("siteId");

	public static final List<String> FIELDS = asList("eventCount", "sumRevenue", "minRevenue", "maxRevenue", "userIds",
			"uniqueUserIds", "estimatedUniqueUserIdCount");

	public static final Map<String, String> OUTPUT_TO_INPUT_FIELDS;

	static {
		OUTPUT_TO_INPUT_FIELDS = new HashMap<>();
		OUTPUT_TO_INPUT_FIELDS.put("eventCount", null);
		OUTPUT_TO_INPUT_FIELDS.put("sumRevenue", "revenue");
		OUTPUT_TO_INPUT_FIELDS.put("minRevenue", "revenue");
		OUTPUT_TO_INPUT_FIELDS.put("maxRevenue", "revenue");
		OUTPUT_TO_INPUT_FIELDS.put("userIds", "userId");
		OUTPUT_TO_INPUT_FIELDS.put("uniqueUserIds", "userId");
		OUTPUT_TO_INPUT_FIELDS.put("estimatedUniqueUserIdCount", "userId");
	}

	@Test
	public void test() throws Exception {
		ExecutorService executorService = Executors.newCachedThreadPool();
		Eventloop eventloop = new Eventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationMetadata aggregationMetadata = new AggregationMetadata(KEYS, FIELDS);
		AggregationStructure structure = new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("siteId", intKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("eventCount", longCount())
						.put("sumRevenue", doubleSum())
						.put("minRevenue", doubleMin())
						.put("maxRevenue", doubleMax())
						.put("userIds", longList())
						.put("uniqueUserIds", longSet())
						.put("estimatedUniqueUserIdCount", hyperLogLog(1024))
						.build());
		Path path = temporaryFolder.newFolder().toPath();
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executorService,
				structure, path);

		Aggregation aggregation = new Aggregation(eventloop, executorService, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage, aggregationMetadata, structure);

		StreamProducers.ofIterable(eventloop, asList(new EventRecord(1, 0.34, 1), new EventRecord(2, 0.42, 3),
				new EventRecord(3, 0.13, 20))).streamTo(aggregation.consumer(EventRecord.class, FIELDS, OUTPUT_TO_INPUT_FIELDS));
		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new EventRecord(2, 0.30, 20), new EventRecord(1, 0.22, 1000),
				new EventRecord(2, 0.91, 33))).streamTo(aggregation.consumer(EventRecord.class, FIELDS, OUTPUT_TO_INPUT_FIELDS));
		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new EventRecord(1, 0.01, 1), new EventRecord(3, 0.88, 20),
				new EventRecord(3, 1.01, 21))).streamTo(aggregation.consumer(EventRecord.class, FIELDS, OUTPUT_TO_INPUT_FIELDS));
		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new EventRecord(1, 0.35, 500), new EventRecord(1, 0.59, 17),
				new EventRecord(2, 0.85, 50))).streamTo(aggregation.consumer(EventRecord.class, FIELDS, OUTPUT_TO_INPUT_FIELDS));
		eventloop.run();

		AggregationQuery query = new AggregationQuery()
				.keys(KEYS)
				.fields(FIELDS);
		StreamConsumers.ToList<QueryResult> listConsumer = StreamConsumers.toList(eventloop);
		aggregation.query(query, QueryResult.class).streamTo(listConsumer);
		eventloop.run();

		double delta = 1E-3;
		List<QueryResult> queryResults = listConsumer.getList();
		assertEquals(3, queryResults.size());

		QueryResult s1 = queryResults.get(0);
		assertEquals(1, s1.siteId);
		assertEquals(5, s1.eventCount);
		assertEquals(1.51, s1.sumRevenue, delta);
		assertEquals(0.01, s1.minRevenue, delta);
		assertEquals(0.59, s1.maxRevenue, delta);
		assertEquals(asList(1L, 1000L, 1L, 500L, 17L), s1.userIds);
		assertEquals(newHashSet(1L, 17L, 500L, 1000L), s1.uniqueUserIds);
		assertEquals(4, s1.estimatedUniqueUserIdCount.estimate());

		QueryResult s2 = queryResults.get(1);
		assertEquals(2, s2.siteId);
		assertEquals(4, s2.eventCount);
		assertEquals(2.48, s2.sumRevenue, delta);
		assertEquals(0.30, s2.minRevenue, delta);
		assertEquals(0.91, s2.maxRevenue, delta);
		assertEquals(asList(3L, 20L, 33L, 50L), s2.userIds);
		assertEquals(newHashSet(3L, 20L, 33L, 50L), s2.uniqueUserIds);
		assertEquals(4, s2.estimatedUniqueUserIdCount.estimate());

		QueryResult s3 = queryResults.get(2);
		assertEquals(3, s3.siteId);
		assertEquals(3, s3.eventCount);
		assertEquals(2.02, s3.sumRevenue, delta);
		assertEquals(0.13, s3.minRevenue, delta);
		assertEquals(1.01, s3.maxRevenue, delta);
		assertEquals(asList(20L, 20L, 21L), s3.userIds);
		assertEquals(newHashSet(20L, 21L), s3.uniqueUserIds);
		assertEquals(2, s3.estimatedUniqueUserIdCount.estimate());
	}
}
