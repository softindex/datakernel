/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.fs.transformers;

import io.datakernel.common.exception.StacklessException;
import io.datakernel.promise.Promise;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.DataFrame;

/**
 * Makes frames from given data and {@link CheckpointStorage}.
 * Unlike {@link FrameSigner} it does not sign anything by itself,
 * it only loads data from given sources.
 * <p>
 * It's counterpart is the {@link FramesIntoStorage}.
 */
public final class FramesFromStorage extends ByteBufsToFrames {
	private final String fileName;
	private final long[] checkpoints;

	private final CheckpointStorage checkpointStorage;
	private final int lastCheckpointIndex;

	private int nextCheckpointIndex;

	private FramesFromStorage(String fileName, CheckpointStorage checkpointStorage,
							  long[] checkpoints, int firstCheckpointIndex, int lastCheckpointIndex) {
		super(checkpoints[firstCheckpointIndex]);
		this.fileName = fileName;
		this.checkpoints = checkpoints;
		this.checkpointStorage = checkpointStorage;
		this.lastCheckpointIndex = lastCheckpointIndex;

		nextCheckpointIndex = firstCheckpointIndex;
	}

	public static FramesFromStorage create(String fileName, CheckpointStorage checkpointStorage,
										   long[] checkpoints, int firstCheckpointIndex, int lastCheckpointIndex) {
		return new FramesFromStorage(fileName, checkpointStorage, checkpoints, firstCheckpointIndex, lastCheckpointIndex);
	}

	@Override
	protected Promise<Void> postCheckpoint() {
		long checkpoint = nextCheckpoint;
		if (nextCheckpointIndex < lastCheckpointIndex) {
			nextCheckpoint = checkpoints[++nextCheckpointIndex];
		}
		return checkpointStorage.load(fileName, checkpoint)
				.thenEx((signedCheckpoint, e) -> {
					if (e != null) {
						// we are loading a checkpoint from a position that was obtained using getCheckpoints,
						// so somewhere in between the file was corrupted, or CheckpointStorage implementation is broken
						return Promise.ofException(new StacklessException(FramesFromStorage.class,
								"No checkpoint at position " + position + " for file " + fileName + " found! Is checkpoint data corrupted?", e));
					}
					return send(DataFrame.of(signedCheckpoint));
				});
	}
}
