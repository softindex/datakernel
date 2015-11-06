package io.datakernel.cube.api;

import java.util.List;

public interface NameResolver {
	List<Object> resolveByKey(List<String> columnNames, List<List<Object>> keys);
}
