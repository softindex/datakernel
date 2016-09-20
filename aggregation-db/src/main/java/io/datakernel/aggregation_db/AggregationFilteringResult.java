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

import java.util.ArrayList;
import java.util.List;

public class AggregationFilteringResult {
	private boolean matches;
	private List<String> appliedPredicateKeys;

	private AggregationFilteringResult(boolean matches) {
		this(matches, new ArrayList<String>());
	}

	private AggregationFilteringResult(boolean matches, List<String> appliedPredicateKeys) {
		this.matches = matches;
		this.appliedPredicateKeys = appliedPredicateKeys;
	}

	public static AggregationFilteringResult create(boolean matches, List<String> appliedPredicateKeys) {
		return new AggregationFilteringResult(matches, appliedPredicateKeys);
	}

	public static AggregationFilteringResult create(boolean matches) {return new AggregationFilteringResult(matches);}

	public boolean isMatches() {
		return matches;
	}

	public List<String> getAppliedPredicateKeys() {
		return appliedPredicateKeys;
	}
}
