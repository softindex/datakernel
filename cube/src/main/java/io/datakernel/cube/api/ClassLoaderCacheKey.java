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

package io.datakernel.cube.api;

import io.datakernel.aggregation_db.AggregationPredicate;

import java.util.List;

public final class ClassLoaderCacheKey {
	final List<String> storedDimensions;
	final AggregationPredicate predicate;
	final List<String> storedMeasures;
	final List<String> computedMeasures;
	final List<String> attributes;

	private ClassLoaderCacheKey(List<String> storedDimensions, AggregationPredicate predicate,
	                            List<String> storedMeasures, List<String> computedMeasures, List<String> attributes) {
		this.storedDimensions = storedDimensions;
		this.predicate = predicate;
		this.storedMeasures = storedMeasures;
		this.computedMeasures = computedMeasures;
		this.attributes = attributes;
	}

	public static ClassLoaderCacheKey create(List<String> storedDimensions, AggregationPredicate predicate,
	                                         List<String> storedMeasures, List<String> computedMeasures,
	                                         List<String> attributes) {
		return new ClassLoaderCacheKey(storedDimensions, predicate, storedMeasures, computedMeasures, attributes);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ClassLoaderCacheKey that = (ClassLoaderCacheKey) o;

		if (!storedDimensions.equals(that.storedDimensions)) return false;
		if (!predicate.equals(that.predicate)) return false;
		if (!storedMeasures.equals(that.storedMeasures)) return false;
		if (!computedMeasures.equals(that.computedMeasures)) return false;
		return attributes.equals(that.attributes);

	}

	@Override
	public int hashCode() {
		int result = storedDimensions.hashCode();
		result = 31 * result + predicate.hashCode();
		result = 31 * result + storedMeasures.hashCode();
		result = 31 * result + computedMeasures.hashCode();
		result = 31 * result + attributes.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "ClassLoaderCacheKey{" +
				"storedDimensions=" + storedDimensions +
				", predicate=" + predicate +
				", storedMeasures=" + storedMeasures +
				", computedMeasures=" + computedMeasures +
				", attributes=" + attributes +
				'}';
	}
}
