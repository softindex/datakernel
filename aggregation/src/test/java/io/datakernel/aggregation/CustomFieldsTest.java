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

package io.datakernel.aggregation;

import com.google.common.base.MoreObjects;
import io.datakernel.aggregation.annotation.Key;
import io.datakernel.aggregation.annotation.Measures;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.aggregation.measure.HyperLogLog;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;

public class CustomFieldsTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Measures({"eventCount"})
	public static class EventRecord {
		// dimensions
		@Key
		public int siteId;

		// measures
		@Measures({"sumRevenue", "minRevenue", "maxRevenue"})
		public double revenue;

		@Measures({"uniqueUserIds", "estimatedUniqueUserIdCount"})
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
					.add("uniqueUserIds", uniqueUserIds)
					.add("estimatedUniqueUserIdCount", estimatedUniqueUserIdCount)
					.toString();
		}
	}

	@Test
	public void test() throws Exception {
		ExecutorService executorService = Executors.newCachedThreadPool();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		Path path = temporaryFolder.newFolder().toPath();
		AggregationChunkStorage aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executorService, new IdGeneratorStub(), path);

		AggregationStructure structure = new AggregationStructure()
				.withKey("siteId", FieldTypes.ofInt())
				.withMeasure("eventCount", count(ofLong()))
				.withMeasure("sumRevenue", sum(ofDouble()))
				.withMeasure("minRevenue", min(ofDouble()))
				.withMeasure("maxRevenue", max(ofDouble()))
				.withMeasure("uniqueUserIds", union(ofLong()))
				.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024));

		Aggregation aggregation = Aggregation.create(eventloop, executorService, classLoader, aggregationChunkStorage, structure)
				.withTemporarySortDir(temporaryFolder.newFolder().toPath());

		StreamProducer<EventRecord> producer = StreamProducers.ofIterable(eventloop, asList(
				new EventRecord(1, 0.34, 1),
				new EventRecord(2, 0.42, 3),
				new EventRecord(3, 0.13, 20)));

		CompletableFuture<AggregationDiff> future = aggregation.consume(producer, EventRecord.class).toCompletableFuture();
		eventloop.run();
		aggregation.getState().apply(future.get());

		producer = StreamProducers.ofIterable(eventloop, asList(
				new EventRecord(2, 0.30, 20),
				new EventRecord(1, 0.22, 1000),
				new EventRecord(2, 0.91, 33)));
		future = aggregation.consume(producer, EventRecord.class).toCompletableFuture();
		eventloop.run();
		aggregation.getState().apply(future.get());

		producer = StreamProducers.ofIterable(eventloop, asList(
				new EventRecord(1, 0.01, 1),
				new EventRecord(3, 0.88, 20),
				new EventRecord(3, 1.01, 21)));
		future = aggregation.consume(producer, EventRecord.class).toCompletableFuture();
		eventloop.run();
		aggregation.getState().apply(future.get());

		producer = StreamProducers.ofIterable(eventloop, asList(
				new EventRecord(1, 0.35, 500),
				new EventRecord(1, 0.59, 17),
				new EventRecord(2, 0.85, 50)));
		future = aggregation.consume(producer, EventRecord.class).toCompletableFuture();
		eventloop.run();
		aggregation.getState().apply(future.get());

		AggregationQuery query = AggregationQuery.create()
				.withKeys("siteId")
				.withMeasures("eventCount", "sumRevenue", "minRevenue", "maxRevenue", "uniqueUserIds", "estimatedUniqueUserIdCount");
		StreamConsumers.ToList<QueryResult> listConsumer = StreamConsumers.toList(eventloop);
		aggregation.query(query, QueryResult.class, DefiningClassLoader.create(classLoader)).streamTo(listConsumer);
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
		assertEquals(newHashSet(1L, 17L, 500L, 1000L), s1.uniqueUserIds);
		assertEquals(4, s1.estimatedUniqueUserIdCount.estimate());

		QueryResult s2 = queryResults.get(1);
		assertEquals(2, s2.siteId);
		assertEquals(4, s2.eventCount);
		assertEquals(2.48, s2.sumRevenue, delta);
		assertEquals(0.30, s2.minRevenue, delta);
		assertEquals(0.91, s2.maxRevenue, delta);
		assertEquals(newHashSet(3L, 20L, 33L, 50L), s2.uniqueUserIds);
		assertEquals(4, s2.estimatedUniqueUserIdCount.estimate());

		QueryResult s3 = queryResults.get(2);
		assertEquals(3, s3.siteId);
		assertEquals(3, s3.eventCount);
		assertEquals(2.02, s3.sumRevenue, delta);
		assertEquals(0.13, s3.minRevenue, delta);
		assertEquals(1.01, s3.maxRevenue, delta);
		assertEquals(newHashSet(20L, 21L), s3.uniqueUserIds);
		assertEquals(2, s3.estimatedUniqueUserIdCount.estimate());
	}

}
