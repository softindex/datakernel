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

import io.datakernel.async.Promise;
import io.datakernel.exception.StacklessException;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.GlobalFsCheckpoint;

import java.util.HashMap;
import java.util.Map;

public final class RuntimeCheckpointStorage implements CheckpointStorage {
	private Map<String, Map<Long, SignedData<GlobalFsCheckpoint>>> storage = new HashMap<>();

	@Override
	public Promise<long[]> getCheckpoints(String filename) {
		Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
		return Promise.of(checkpoints != null ? checkpoints.keySet().stream().mapToLong(Long::longValue).sorted().toArray() : new long[0]);
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position) {
		Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
		return checkpoints != null ?
				Promise.of(checkpoints.get(position)) :
				Promise.ofException(new StacklessException(CheckpointStorage.class, "No checkpoint found on position " + position));
	}

	@Override
	public Promise<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint) {
		Map<Long, SignedData<GlobalFsCheckpoint>> fileCheckpoints = storage.computeIfAbsent(filename, $ -> new HashMap<>());
		long pos = checkpoint.getValue().getPosition();
		SignedData<GlobalFsCheckpoint> existing = fileCheckpoints.get(pos);
		if (existing == null) {
			fileCheckpoints.put(pos, checkpoint);
		} else if (!existing.equals(checkpoint)) {
			return Promise.ofException(new StacklessException(CheckpointStorage.class, "Trying to override existing checkpoint at " + pos));
		}
		return Promise.complete();
	}
}
