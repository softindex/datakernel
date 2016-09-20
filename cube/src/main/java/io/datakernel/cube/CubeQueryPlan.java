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
	private boolean optimizedAwayReducer;

	private CubeQueryPlan() {}

	static CubeQueryPlan create() {return new CubeQueryPlan();}

	public void addAggregationMeasures(Aggregation aggregation, List<String> measures) {
		aggregationToMeasures.put(aggregation, measures);
	}

	public void setOptimizedAwayReducer(boolean optimizedAwayReducer) {
		this.optimizedAwayReducer = optimizedAwayReducer;
	}

	public int getNumberOfAggregations() {
		return aggregationToMeasures.size();
	}

	@Override
	public String toString() {
		if (aggregationToMeasures.isEmpty())
			return "empty";

		StringBuilder sb = new StringBuilder();

		for (Map.Entry<Aggregation, List<String>> entry : aggregationToMeasures.entrySet()) {
			sb.append("{").append("aggregation=").append(entry.getKey());
			sb.append(", measures=").append(entry.getValue()).append("} ");
		}

		sb.append("\nOptimized away reducer: ").append(optimizedAwayReducer).append(". ");

		return sb.toString();
	}
}
