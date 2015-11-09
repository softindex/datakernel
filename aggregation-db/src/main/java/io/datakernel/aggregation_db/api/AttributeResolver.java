package io.datakernel.aggregation_db.api;

import io.datakernel.aggregation_db.PrimaryKey;

import java.util.List;
import java.util.Map;

public interface AttributeResolver {
	Map<PrimaryKey, Object> resolve(List<PrimaryKey> keys);
}
