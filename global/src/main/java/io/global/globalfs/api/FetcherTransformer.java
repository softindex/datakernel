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
		long checkpoint = nextCheckpoint;
		if (nextCheckpointIndex < lastCheckpointIndex) {
			nextCheckpoint = checkpoints[++nextCheckpointIndex];
		}
		return checkpointStorage.loadCheckpoint(fileName, checkpoint)
				.thenComposeEx((signedCheckpoint, e) -> {
					if (e != null || signedCheckpoint == null) {
						// we are loading a checkpoint from a position that obtained using getCheckpoints,
						// so somewhere in between the file was corrupted, or CheckpointStorage implementation is broken
						output.closeWithError(new GlobalFsException("No checkpoint at position {} for file {} found! Are checkpoint files corrupted?"));
						return Stage.complete();
					}
					return output.accept(DataFrame.of(signedCheckpoint));
				});
	}

	@Override
	protected void iteration() {
		input.get()
				.whenComplete((buf, e) -> {
					if (e != null) {
						closeWithError(e);
					}
					if (buf == null) {
						output.accept(null)
								.thenRun(this::completeProcess);
						return;
					}
					handleBuffer(buf)
							.whenComplete(($, e2) -> {
								if (e != null) {
									closeWithError(e2);
								}
								iteration();
							});
				});
	}
}
