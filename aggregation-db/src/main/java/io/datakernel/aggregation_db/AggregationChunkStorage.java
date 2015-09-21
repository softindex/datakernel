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

import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.List;

/**
 * Manages persistence of aggregations (chunks of data).
 */
public interface AggregationChunkStorage {
	/**
	 * Creates a {@code StreamProducer} that streams records contained in the chunk.
	 * The chunk to read is determined by {@code aggregationId} and {@code id}.
	 *
	 * @param aggregationId id of aggregation that contains the chunk
	 * @param keys          keys of chunk record
	 * @param fields        fields of chunk record
	 * @param recordClass   class of chunk record
	 * @param id            id of chunk
	 * @return StreamProducer, which will stream read records to its wired consumer.
	 */
	<T> StreamProducer<T> chunkReader(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id);

	/**
	 * Creates a {@code StreamConsumer} that persists streamed records.
	 * The chunk to write is determined by {@code aggregationId} and {@code id}.
	 *
	 * @param aggregationId id of aggregation that contains the chunk
	 * @param keys          keys of chunk record
	 * @param fields        fields of chunk record
	 * @param recordClass   class of chunk record
	 * @param id            id of chunk
	 * @return StreamConsumer, which will write records, streamed from wired producer.
	 */
	<T> StreamConsumer<T> chunkWriter(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id);

	/**
	 * Removes the chunk determined by {@code aggregationId} and {@code id}.
	 *
	 * @param aggregationId id of aggregation that contains the chunk
	 * @param id            id of chunk
	 */
	void removeChunk(String aggregationId, long id);
}
