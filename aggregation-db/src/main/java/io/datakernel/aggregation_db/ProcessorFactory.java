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

import io.datakernel.stream.processor.StreamReducers;

import java.util.List;

/**
 * Represents a factory of reducers and preaggregator, which define the data aggregation logic.
 */
public interface ProcessorFactory {
	StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass,
	                                          List<String> keys, List<String> inputFields, List<String> outputFields);

	Aggregate createPreaggregator(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                              List<String> inputFields, List<String> outputFields);
}
