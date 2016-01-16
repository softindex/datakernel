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
import io.datakernel.aggregation_db.fieldtype.FieldTypeInt;
import io.datakernel.aggregation_db.fieldtype.FieldTypeLong;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeInt;
import io.datakernel.async.AsyncExecutors;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class KeyValueTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static class KeyValuePair {
		public int key;
		public int value;
		public long timestamp;

		public KeyValuePair() {
		}

		public KeyValuePair(int key, int value) {
			this(key, value, System.currentTimeMillis());
		}

		public KeyValuePair(int key, int value, long timestamp) {
			this.key = key;
			this.value = value;
			this.timestamp = timestamp;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			KeyValuePair that = (KeyValuePair) o;

			if (key != that.key) return false;
			if (value != that.value) return false;
			return timestamp == that.timestamp;

		}

		@Override
		public int hashCode() {
			int result = key;
			result = 31 * result + value;
			result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
			return result;
		}

		public static final List<String> KEYS = asList("key");

		public static final List<String> FIELDS = asList("value", "timestamp");

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("key", key)
					.add("value", value)
					.add("timestamp", timestamp)
					.toString();
		}
	}

	@Test
	public void testKeyValue() throws Exception {
		ExecutorService executorService = Executors.newCachedThreadPool();
		Eventloop eventloop = new Eventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationMetadata aggregationMetadata = new AggregationMetadata("key-value", KeyValuePair.KEYS, KeyValuePair.FIELDS);
		ProcessorFactory keyValueProcessorFactory = new KeyValueProcessorFactory(classLoader);
		AggregationStructure aggregationStructure = new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("key", new KeyTypeInt())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("value", new FieldTypeInt())
						.put("timestamp", new FieldTypeLong())
						.build());
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executorService,
				AsyncExecutors.sequentialExecutor(), aggregationStructure, temporaryFolder.newFolder().toPath());

		Aggregation aggregation = new Aggregation(eventloop, classLoader, aggregationMetadataStorage, aggregationChunkStorage, aggregationMetadata,
				aggregationStructure, keyValueProcessorFactory);

		StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 1, 0), new KeyValuePair(1, 2, 1), new KeyValuePair(1, 1, 2)))
				.streamTo(aggregation.consumer(KeyValuePair.class));

		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(2, 1, 3), new KeyValuePair(2, 2, 4), new KeyValuePair(2, 15, 5)))
				.streamTo(aggregation.consumer(KeyValuePair.class));

		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(3, 4, 6), new KeyValuePair(3, 6, 7), new KeyValuePair(1, 0, 8)))
				.streamTo(aggregation.consumer(KeyValuePair.class));

		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new KeyValuePair(1, 15, 9), new KeyValuePair(2, 21, 10)))
				.streamTo(aggregation.consumer(KeyValuePair.class));

		eventloop.run();

		AggregationQuery query = new AggregationQuery()
				.keys(KeyValuePair.KEYS)
				.fields(KeyValuePair.FIELDS);
		StreamConsumers.ToList<KeyValuePair> consumerToList = StreamConsumers.toList(eventloop);
		aggregation.query(query, KeyValuePair.class).streamTo(consumerToList);

		eventloop.run();

		List<KeyValuePair> expectedResult = asList(new KeyValuePair(1, 15, 9), new KeyValuePair(2, 21, 10), new KeyValuePair(3, 6, 7));
		List<KeyValuePair> actualResult = consumerToList.getList();

		assertEquals(expectedResult, actualResult);
	}
}
