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

import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class DrillDown {
	private final List<String> chain;
	private final Set<String> measures;

	public DrillDown(List<String> chain, Set<String> measures) {
		this.chain = chain;
		this.measures = measures;
	}

	public List<String> getChain() {
		return chain;
	}

	public Set<String> getMeasures() {
		return measures;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		DrillDown drillDown1 = (DrillDown) o;
		return Objects.equals(chain, drillDown1.chain) &&
				Objects.equals(measures, drillDown1.measures);
	}

	@Override
	public int hashCode() {
		return Objects.hash(chain, measures);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("chain", chain)
				.add("measures", measures)
				.toString();
	}
}
