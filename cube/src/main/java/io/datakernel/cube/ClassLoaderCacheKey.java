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

import java.util.Set;

final class ClassLoaderCacheKey {
	final Set<String> attributes;
	final Set<String> measures;
	final Set<String> filterDimensions;

	ClassLoaderCacheKey(Set<String> attributes, Set<String> measures, Set<String> filterDimensions) {
		this.attributes = attributes;
		this.measures = measures;
		this.filterDimensions = filterDimensions;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ClassLoaderCacheKey that = (ClassLoaderCacheKey) o;

		if (!attributes.equals(that.attributes)) return false;
		if (!measures.equals(that.measures)) return false;
		if (!filterDimensions.equals(that.filterDimensions)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = attributes.hashCode();
		result = 31 * result + measures.hashCode();
		result = 31 * result + filterDimensions.hashCode();
		return result;
	}
}
