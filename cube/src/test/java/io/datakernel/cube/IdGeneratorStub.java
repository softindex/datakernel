package io.datakernel.cube;

import io.datakernel.aggregation.IdGenerator;
import io.datakernel.async.Stage;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long chunkId;

	@Override
	public Stage<Long> createId() {
		return Stage.of(++chunkId);
	}
}
