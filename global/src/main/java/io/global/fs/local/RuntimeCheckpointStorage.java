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

package io.global.fs.local;

import io.datakernel.async.Stage;
import io.datakernel.exception.StacklessException;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.GlobalFsCheckpoint;

import java.util.HashMap;
import java.util.Map;

public final class RuntimeCheckpointStorage implements CheckpointStorage {
	private Map<String, Map<Long, SignedData<GlobalFsCheckpoint>>> storage = new HashMap<>();

	@Override
	public Stage<long[]> getCheckpoints(String filename) {
		Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
		if (checkpoints == null) {
			return Stage.of(new long[0]);
		}
		return Stage.of(checkpoints.keySet().stream().mapToLong(Long::longValue).sorted().toArray());
	}

	@Override
	public Stage<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position) {
		Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
		if (checkpoints == null) {
			return Stage.of(null);
		}
		return Stage.of(checkpoints.get(position));
	}

	@Override
	public Stage<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint) {
		Map<Long, SignedData<GlobalFsCheckpoint>> fileCheckpoints = storage.computeIfAbsent(filename, $ -> new HashMap<>());
		long pos = checkpoint.getData().getPosition();
		SignedData<GlobalFsCheckpoint> existing = fileCheckpoints.get(pos);
		if (existing == null) {
			fileCheckpoints.put(pos, checkpoint);
		} else if (!existing.equals(checkpoint)) {
			return Stage.ofException(new StacklessException(RuntimeCheckpointStorage.class, "Trying to override existing checkpoint"));
		}
		return Stage.of(null);
	}
}
