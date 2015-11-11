package io.datakernel.cube.api;

import io.datakernel.aggregation_db.PrimaryKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AttributeResolver {
	Map<PrimaryKey, Object[]> resolve(Set<PrimaryKey> keys, List<String> attributes);
}
