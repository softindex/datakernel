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
import io.global.fs.util.Globs;

import java.util.*;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

public final class RuntimeCheckpointStorage implements CheckpointStorage {
	private Map<String, Map<Long, SignedData<GlobalFsCheckpoint>>> storage = new HashMap<>();

	@Override
	public Promise<long[]> loadIndex(String filename) {
		Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
		return Promise.of(checkpoints != null ? checkpoints.keySet().stream().mapToLong(Long::longValue).sorted().toArray() : new long[0]);
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> load(String filename, long position) {
		Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
		return checkpoints != null ?
				Promise.of(checkpoints.get(position)) :
				Promise.ofException(NO_CHECKPOINT);
	}

	@Override
	public Promise<List<String>> listMetaCheckpoints(String glob) {
		Predicate<String> pred = Globs.getGlobStringPredicate(glob);
		return Promise.of(storage.entrySet()
				.stream()
				.filter(e -> pred.test(e.getKey()))
				.map(e -> e.getValue()
						.entrySet()
						.stream()
						.max(Comparator.comparingLong(Map.Entry::getKey)))
				.filter(Optional::isPresent)
				.map(e -> e.get().getValue().getValue().getFilename())
				.collect(toList()));
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> loadMetaCheckpoint(String filename) {
		Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
		if (checkpoints != null) {
			Optional<SignedData<GlobalFsCheckpoint>> maybeCheckpoint = checkpoints.entrySet()
					.stream()
					.max(Comparator.comparingLong(Map.Entry::getKey))
					.map(Map.Entry::getValue);
			if (maybeCheckpoint.isPresent()) {
				return Promise.of(maybeCheckpoint.get());
			}
		}
		return Promise.ofException(NO_CHECKPOINT);
	}

	@Override
	public Promise<Void> store(String filename, SignedData<GlobalFsCheckpoint> signedCheckpoint) {
		GlobalFsCheckpoint checkpoint = signedCheckpoint.getValue();
		Map<Long, SignedData<GlobalFsCheckpoint>> fileCheckpoints = storage.computeIfAbsent(filename, $ -> new HashMap<>());
		if (checkpoint.isTombstone()) {
			fileCheckpoints.clear();
			fileCheckpoints.put(0L, signedCheckpoint);
			return Promise.complete();
		}
		long pos = checkpoint.getPosition();
		SignedData<GlobalFsCheckpoint> existing = fileCheckpoints.get(pos);
		if (existing == null) {
			fileCheckpoints.put(pos, signedCheckpoint);
		} else if (!existing.equals(signedCheckpoint)) {
			return Promise.ofException(new StacklessException(CheckpointStorage.class, "Trying to override existing checkpoint at " + pos));
		}
		return Promise.complete();
	}

	@Override
	public Promise<Void> drop(String filename) {
		storage.remove(filename);
		return Promise.complete();
	}
}
