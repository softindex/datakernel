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

package io.datakernel.jmx;

/**
 * Common interface for accumulating different type of values
 * <p/>
 * It should have one or more getter, annotated with @JmxAttribute annotation.
 * Such methods are used for fetching values and are allowed to throw {@link AggregationException}
 *
 * @param <T> type of value to accumulate
 */
public interface JmxAccumulator<T> {
	void add(T value);

	// TODO (vmykhalko): start here
	// TODO (vmykhalko): is getAttributes() method essential ? if not this class can be renamed to JmxAccumulator
//	SortedMap<String, TypeAndValue> getAttributes();
}
