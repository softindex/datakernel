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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class CubeQueryPlan {
	private Map<Aggregation, List<String>> aggregationToMeasures = new LinkedHashMap<>();

	public void addAggregationMeasures(Aggregation aggregation, List<String> measures) {
		aggregationToMeasures.put(aggregation, measures);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (Map.Entry<Aggregation, List<String>> entry : aggregationToMeasures.entrySet()) {
			sb.append("{");
			sb.append("aggregation=").append(entry.getKey());
			sb.append(", measures=").append(entry.getValue());
			sb.append("} ");
		}

		return sb.toString();
	}
}
