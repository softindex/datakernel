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
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.measure.Measures.union;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class InvertedIndexTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static class InvertedIndexQueryResult {
		public String word;
		public Set<Integer> documents;

		public InvertedIndexQueryResult() {
		}

		public InvertedIndexQueryResult(String word, Set<Integer> documents) {
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationMetadataStorageStub metadataStorage = new AggregationMetadataStorageStub(eventloop);
		Path path = temporaryFolder.newFolder().toPath();
		AggregationChunkStorage aggregationChunkStorage = LocalFsChunkStorage.create(eventloop, executorService, path);

		Aggregation aggregation = Aggregation.create(eventloop, executorService, classLoader, metadataStorage, aggregationChunkStorage)
				.withKey("word", FieldTypes.ofString())
				.withMeasure("documents", union(ofInt()));

		StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2), new InvertedIndexRecord("fox", 3)))
				.streamTo(aggregation.consumer(InvertedIndexRecord.class,
						InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS,
						metadataStorage.createSaveCallback()));
		eventloop.run();

		aggregation.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("brown", 3),
				new InvertedIndexRecord("lazy", 4), new InvertedIndexRecord("dog", 1)))
				.streamTo(aggregation.consumer(InvertedIndexRecord.class,
						InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS,
						metadataStorage.createSaveCallback()));
		eventloop.run();

		StreamProducers.ofIterable(eventloop, asList(new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4), new InvertedIndexRecord("brown", 10)))
				.streamTo(aggregation.consumer(InvertedIndexRecord.class,
						InvertedIndexRecord.OUTPUT_FIELDS, InvertedIndexRecord.OUTPUT_TO_INPUT_FIELDS,
						metadataStorage.createSaveCallback()));
		eventloop.run();

		aggregation.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		AggregationQuery query = AggregationQuery.create()
				.withKeys(InvertedIndexRecord.KEYS)
				.withFields(InvertedIndexRecord.OUTPUT_FIELDS);
		StreamConsumers.ToList<InvertedIndexQueryResult> consumerToList = StreamConsumers.toList(eventloop);
		aggregation.query(query, InvertedIndexQueryResult.class, classLoader).streamTo(consumerToList);
		eventloop.run();

		System.out.println(consumerToList.getList());

		List<InvertedIndexQueryResult> expectedResult = asList(new InvertedIndexQueryResult("brown", newHashSet(2, 3, 10)),
				new InvertedIndexQueryResult("dog", newHashSet(1)), new InvertedIndexQueryResult("fox", newHashSet(1, 3, 4)),
				new InvertedIndexQueryResult("lazy", newHashSet(4)), new InvertedIndexQueryResult("quick", newHashSet(1)));
		List<InvertedIndexQueryResult> actualResult = consumerToList.getList();

		assertEquals(expectedResult, actualResult);
	}
}
