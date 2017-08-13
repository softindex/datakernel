package io.datakernel.cube;

import io.datakernel.aggregation.IdGenerator;
import io.datakernel.async.ResultCallback;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long id;

	@Override
	public void createId(ResultCallback<Long> callback) {
		callback.setResult(++id);
	}
}
