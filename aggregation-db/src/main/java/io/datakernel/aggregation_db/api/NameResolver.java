package io.datakernel.aggregation_db.api;

import java.util.List;

public interface NameResolver {
	List<Object> resolveByKey(List<String> keyNames, List<List<Object>> keys);
}
