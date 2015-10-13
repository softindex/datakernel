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

import io.datakernel.async.CompletionCallback;

/**
 * Manages persistence of cube metadata.
 */
public interface CubeMetadataStorage {
	/**
	 * Loads aggregations metadata from metadata storage to specified cube asynchronously.
	 *
	 * @param cube     cube where retrieved aggregations metadata is to be saved
	 * @param callback callback which is called once loading is complete
	 */
	void loadAggregations(Cube cube, CompletionCallback callback);

	/**
	 * Saves aggregations metadata from the specified cube to metadata storage asynchronously.
	 *
	 * @param cube     cube whose aggregations metadata is to be saved
	 * @param callback callback which is called once saving is complete
	 */
	void saveAggregations(Cube cube, CompletionCallback callback);
}
