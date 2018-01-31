package io.datakernel.aggregation;

import io.datakernel.async.Stage;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long chunkId;

	@Override
	public Stage<Long> createId() {
		return Stage.of(++chunkId);
	}
}
