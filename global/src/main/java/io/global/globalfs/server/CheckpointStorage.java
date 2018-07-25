package io.global.globalfs.server;

import io.datakernel.async.Stage;
import io.global.common.SignedData;
import io.global.globalfs.api.GlobalFsCheckpoint;

import java.util.Arrays;

public interface CheckpointStorage {
	Stage<long[]> getCheckpoints(String filename);

	default Stage<Long> getLastCheckpoint(String filename) {
		return getCheckpoints(filename)
				.thenApply(positions -> Arrays.stream(positions).max().orElse(0L));
	}

	Stage<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position);

	Stage<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint);
}
