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

import com.google.common.base.Objects;

import java.util.List;
import java.util.Set;

public final class AvailableDrillDowns {
	private final Set<List<String>> drillDowns;
	private final Set<String> measures;

	public AvailableDrillDowns(Set<List<String>> drillDowns, Set<String> measures) {
		this.drillDowns = drillDowns;
		this.measures = measures;
	}

	public Set<List<String>> getDrillDowns() {
		return drillDowns;
	}

	public Set<String> getMeasures() {
		return measures;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AvailableDrillDowns that = (AvailableDrillDowns) o;

		if (drillDowns != null ? !drillDowns.equals(that.drillDowns) : that.drillDowns != null) return false;
		return !(measures != null ? !measures.equals(that.measures) : that.measures != null);
	}

	@Override
	public int hashCode() {
		int result = drillDowns != null ? drillDowns.hashCode() : 0;
		result = 31 * result + (measures != null ? measures.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("drillDowns", drillDowns)
				.add("measures", measures)
				.toString();
	}
}
