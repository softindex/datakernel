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
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.examples.InvertedIndexRecord;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation_db.fieldtype.FieldTypes.intList;
import static io.datakernel.aggregation_db.keytype.KeyTypes.stringKey;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class InvertedIndexTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static class InvertedIndexQueryResult {
		public String word;
		public List<Integer> documents;

		public InvertedIndexQueryResult() {
		}

		public InvertedIndexQueryResult(String word, List<Integer> documents) {
			this.word = word;
			this.documents = documents;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			InvertedIndexQueryResult that = (InvertedIndexQueryResult) o;

			if (word != null ? !word.equals(that.word) : that.word != null) return false;
			return !(documents != null ? !documents.equals(that.documents) : that.documents != null);

		}

		@Override
		public int hashCode() {
			int result = word != null ? word.hashCode() : 0;
			result = 31 * result + (documents != null ? documents.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("word", word)
					.add("documents", documents)
					.toString();
		}
	}

	@Test
	public void testInvertedIndex() throws Exception {
		ExecutorService executorService = Executors.newCachedThreadPool();
		Eventloop eventloop = new Eventloop();
		DefiningClassLoader classLoader = new DefiningClassLoader();
		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationMetadata aggregationMetadata = new AggregationMetadata(InvertedIndexRecord.KEYS,
				InvertedIndexRecord.OUTPUT_FIELDS);
		AggregationStructure structure = new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("word", stringKey())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("documents", intList())
						.build());
		Path path = temporaryFolder.newFolder().toPath();
		AggregationChunkStorage aggregationChunkStorage = new LocalFsChunkStorage(eventloop, executorService,
				structure, path);

		Aggregation aggregation = new Aggregation(eventloop, executorService, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage, aggregationMetadata, structure);

		StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2), new InvertedIndexRecord("fox", 3)))
				.streamTo(aggregation.consumer(InvertedIndexRecord.class,
						InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS));

		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("brown", 3),
				new InvertedIndexRecord("lazy", 4), new InvertedIndexRecord("dog", 1)))
				.streamTo(aggregation.consumer(InvertedIndexRecord.class,
						InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS));

		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4), new InvertedIndexRecord("brown", 10)))
				.streamTo(aggregation.consumer(InvertedIndexRecord.class,
						InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS));

		eventloop.run();

		AggregationQuery query = new AggregationQuery()
				.keys(InvertedIndexRecord.KEYS)
				.fields(InvertedIndexRecord.OUTPUT_FIELDS);

		StreamConsumers.ToList<InvertedIndexQueryResult> consumerToList = StreamConsumers.toList(eventloop);
		aggregation.query(query, InvertedIndexQueryResult.class).streamTo(consumerToList);

		eventloop.run();

		System.out.println(consumerToList.getList());

		List<InvertedIndexQueryResult> expectedResult = asList(new InvertedIndexQueryResult("brown", asList(2, 3, 10)),
				new InvertedIndexQueryResult("dog", asList(1)), new InvertedIndexQueryResult("fox", asList(1, 3, 4)),
				new InvertedIndexQueryResult("lazy", asList(4)), new InvertedIndexQueryResult("quick", asList(1)));
		List<InvertedIndexQueryResult> actualResult = consumerToList.getList();

		assertEquals(expectedResult, actualResult);
	}
}
