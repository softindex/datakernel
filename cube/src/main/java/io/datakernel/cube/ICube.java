package io.datakernel.cube;

import io.datakernel.aggregation.QueryException;
import io.datakernel.async.ResultCallback;

import java.lang.reflect.Type;
import java.util.Map;

public interface ICube {
	void query(CubeQuery cubeQuery, ResultCallback<QueryResult> resultCallback) throws QueryException;

	Map<String, Type> getAttributeTypes();

	Map<String, Type> getMeasureTypes();
}
