package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.global.globalfs.server.CheckpointStorage;

public class FetcherTransformer extends ByteBufsToFramesTransformer {
	private final String fileName;
	private final long[] checkpoints;

	private final CheckpointStorage checkpointStorage;
	private final int lastCheckpointIndex;

	private int nextCheckpointIndex;

	private FetcherTransformer(String fileName, CheckpointStorage checkpointStorage,
			long[] checkpoints, int firstCheckpointIndex, int lastCheckpointIndex) {
		super(checkpoints[firstCheckpointIndex]);
		this.fileName = fileName;
		this.checkpoints = checkpoints;
		this.checkpointStorage = checkpointStorage;
		this.lastCheckpointIndex = lastCheckpointIndex;

		nextCheckpointIndex = firstCheckpointIndex;
	}

	public static FetcherTransformer create(String fileName, CheckpointStorage checkpointStorage,
			long[] checkpoints, int firstCheckpointIndex, int lastCheckpointIndex) {
		return new FetcherTransformer(fileName, checkpointStorage, checkpoints, firstCheckpointIndex, lastCheckpointIndex);
	}

	@Override
	protected Stage<Void> postNextCheckpoint() {
		Stage<Void> stage = checkpointStorage.loadCheckpoint(fileName, nextCheckpoint)
				.thenCompose(c -> output.accept(DataFrame.of(c)));
		if (nextCheckpointIndex < lastCheckpointIndex) {
			nextCheckpoint = checkpoints[++nextCheckpointIndex];
			return stage;
		}
		return stage.thenRun(() -> output.accept(null));
	}

	@Override
	protected void iteration() {
		input.get()
				.whenResult(buf -> {
					if (buf == null) {
						return;
					}
					handleBuffer(buf)
							.whenComplete(($, e) -> {
								if (e != null) {
									output.closeWithError(e);
								}
								iteration();
							});
				});
	}
}
