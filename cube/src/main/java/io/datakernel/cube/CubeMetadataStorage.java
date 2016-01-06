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

package io.datakernel.cube;

import io.datakernel.aggregation_db.Aggregation;
import io.datakernel.aggregation_db.AggregationMetadata;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.Collection;
import java.util.List;

/**
 * Manages persistence of cube metadata.
 */
public interface CubeMetadataStorage {
	/**
	 * Loads aggregations metadata from metadata storage.
	 *
	 * @param structure          aggregation structure
	 * @param callback           callback which is called once loading is complete
	 */
	void loadAggregations(AggregationStructure structure, ResultCallback<List<AggregationMetadata>> callback);

	/**
	 * Saves specified aggregations.
	 *
	 * @param structure    aggregation structure
	 * @param aggregations aggregations to save
	 * @param callback     callback which is called once saving is complete
	 */
	void saveAggregations(AggregationStructure structure, Collection<Aggregation> aggregations,
	                      CompletionCallback callback);
}
