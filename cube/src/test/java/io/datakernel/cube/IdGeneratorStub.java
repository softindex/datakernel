package io.datakernel.cube;

import io.datakernel.aggregation.IdGenerator;
import io.datakernel.async.Stages;

import java.util.concurrent.CompletionStage;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long chunkId;

	@Override
	public CompletionStage<Long> createId() {
		return Stages.of(++chunkId);
	}
}
