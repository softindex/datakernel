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

import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.async.Stage;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;

import java.util.List;
import java.util.Set;

/**
 * Manages persistence of aggregations (chunks of data).
 */
public interface AggregationChunkStorage<C> extends IdGenerator<C> {
	/**
	 * Creates a {@code StreamProducer} that streams records contained in the chunk.
	 * The chunk to read is determined by {@code aggregationId} and {@code id}.
	 *
	 * @param recordClass class of chunk record
	 * @param chunkId     id of chunk
	 * @return StreamProducer, which will stream read records to its wired consumer.
	 */
	<T> Stage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId, DefiningClassLoader classLoader);

	default <T> StreamProducerWithResult<T, Void> readStream(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId, DefiningClassLoader classLoader) {
		return StreamProducerWithResult.ofStage(read(aggregation, fields, recordClass, chunkId, classLoader));
	}

	/**
	 * Creates a {@code StreamConsumer} that persists streamed records.
	 * The chunk to write is determined by {@code aggregationId} and {@code id}.
	 *
	 * @param fields      fields of chunk record
	 * @param recordClass class of chunk record
	 * @param chunkId     id of chunk
	 */
	<T> Stage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId, DefiningClassLoader classLoader);

	default <T> StreamConsumerWithResult<T, Void> writeStream(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId, DefiningClassLoader classLoader) {
		return StreamConsumerWithResult.ofStage(write(aggregation, fields, recordClass, chunkId, classLoader));
	}

	Stage<Void> finish(Set<C> chunkIds);

}


