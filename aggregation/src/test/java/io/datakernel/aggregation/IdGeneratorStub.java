package io.datakernel.aggregation;

import java.util.concurrent.CompletionStage;

import static io.datakernel.async.SettableStage.immediateStage;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long chunkId;

	@Override
	public CompletionStage<Long> createId() {
		return immediateStage(++chunkId);
	}
}
