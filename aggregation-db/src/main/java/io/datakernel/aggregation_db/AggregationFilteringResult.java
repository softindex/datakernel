package io.datakernel.aggregation_db;

import java.util.ArrayList;
import java.util.List;

public class AggregationFilteringResult {
	private boolean matches;
	private List<String> appliedPredicateKeys;

	public AggregationFilteringResult(boolean matches) {
		this(matches, new ArrayList<String>());
	}

	public AggregationFilteringResult(boolean matches, List<String> appliedPredicateKeys) {
		this.matches = matches;
		this.appliedPredicateKeys = appliedPredicateKeys;
	}

	public boolean isMatches() {
		return matches;
	}

	public List<String> getAppliedPredicateKeys() {
		return appliedPredicateKeys;
	}
}
