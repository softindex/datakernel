package io.datakernel.cube;

import io.datakernel.aggregation.IdGenerator;
import io.datakernel.promise.Promise;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long chunkId;

	@Override
	public Promise<Long> createId() {
		return Promise.of(++chunkId);
	}
}
