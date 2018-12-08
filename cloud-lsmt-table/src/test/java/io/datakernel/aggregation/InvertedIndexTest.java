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

import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.StreamSupplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofString;
import static io.datakernel.aggregation.measure.Measures.union;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.util.CollectionUtils.set;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
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
			return "InvertedIndexQueryResult{" +
					"word='" + word + '\'' +
					", documents=" + documents +
					'}';
		}
	}

	@Test
	public void testInvertedIndex() throws Exception {
		ExecutorService executorService = Executors.newCachedThreadPool();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Path path = temporaryFolder.newFolder().toPath();
		AggregationChunkStorage<Long> aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, executorService, path));

		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
				.withKey("word", ofString())
				.withMeasure("documents", union(ofInt()));

		Aggregation aggregation = Aggregation.create(eventloop, executorService, classLoader, aggregationChunkStorage, structure)
				.withTemporarySortDir(temporaryFolder.newFolder().toPath());

		StreamSupplier<InvertedIndexRecord> supplier = StreamSupplier.of(
				new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2),
				new InvertedIndexRecord("fox", 3));
		CompletableFuture<AggregationDiff> future = aggregation.consume(supplier, InvertedIndexRecord.class).toCompletableFuture();
		eventloop.run();
		aggregation.getState().apply(future.get());

		aggregationChunkStorage.finish(getAddedChunks(future.get()));
		eventloop.run();

		supplier = StreamSupplier.of(
				new InvertedIndexRecord("brown", 3),
				new InvertedIndexRecord("lazy", 4),
				new InvertedIndexRecord("dog", 1));
		future = aggregation.consume(supplier, InvertedIndexRecord.class).toCompletableFuture();
		eventloop.run();
		aggregation.getState().apply(future.get());

		aggregationChunkStorage.finish(getAddedChunks(future.get()));
		eventloop.run();

		supplier = StreamSupplier.of(
				new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4),
				new InvertedIndexRecord("brown", 10));
		future = aggregation.consume(supplier, InvertedIndexRecord.class).toCompletableFuture();
		eventloop.run();
		aggregation.getState().apply(future.get());

		aggregationChunkStorage.finish(getAddedChunks(future.get()));
		eventloop.run();

		AggregationQuery query = AggregationQuery.create()
				.withKeys("word")
				.withMeasures("documents");

		CompletableFuture<List<InvertedIndexQueryResult>> future1 =
				aggregation.query(query, InvertedIndexQueryResult.class, DefiningClassLoader.create(classLoader))
						.toList()
						.toCompletableFuture();

		eventloop.run();

		List<InvertedIndexQueryResult> expectedResult = asList(
				new InvertedIndexQueryResult("brown", set(2, 3, 10)),
				new InvertedIndexQueryResult("dog", set(1)),
				new InvertedIndexQueryResult("fox", set(1, 3, 4)),
				new InvertedIndexQueryResult("lazy", set(4)),
				new InvertedIndexQueryResult("quick", set(1)));
		List<InvertedIndexQueryResult> actualResult = future1.get();

		assertEquals(expectedResult, actualResult);
	}

	private Set<Long> getAddedChunks(AggregationDiff aggregationDiff) {
		return aggregationDiff.getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(toSet());
	}

}
