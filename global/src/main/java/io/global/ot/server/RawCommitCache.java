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

package io.global.ot.server;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.exception.ToDoException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;

import java.util.HashMap;
import java.util.Map;

final class RawCommitCache implements AsyncConsumer<RawCommitEntry> {
	private final Map<CommitId, RawCommit> preloadedCommits = new HashMap<>();
	private final Map<CommitId, SettableStage<RawCommit>> pendingStages = new HashMap<>();
	private SettableStage<Void> acceptStage;

	public RawCommitCache() {
	}

	public static RawCommitCache of(SerialSupplier<RawCommitEntry> supplier) {
		RawCommitCache cache = new RawCommitCache();
		supplier.streamTo(SerialConsumer.of(cache))
				.whenResult($ -> cache.onEndOfStream())
				.whenException(cache::onError);
		return cache;
	}

	public void onEndOfStream() {
		closePendingStages(new ToDoException());
	}

	public void onError(Throwable throwable) {
		closePendingStages(throwable);
		preloadedCommits.clear();
	}

	private void closePendingStages(Throwable throwable) {
		pendingStages.values().forEach(pendingStage -> pendingStage.setException(throwable));
		pendingStages.clear();
	}

	@Override
	public Stage<Void> accept(RawCommitEntry entry) {
		SettableStage<RawCommit> pendingStage = pendingStages.remove(entry.commitId);
		if (pendingStage != null) {
			pendingStage.set(entry.rawCommit);
			return Stage.complete();
		}
		preloadedCommits.put(entry.commitId, entry.rawCommit);
		if (acceptStage != null) {
			acceptStage = new SettableStage<>();
		}
		return acceptStage;
	}

	public Stage<RawCommit> loadCommit(CommitId commitId) {
		RawCommit rawCommit = preloadedCommits.remove(commitId);
		if (rawCommit != null) {
			return Stage.of(rawCommit);
		}
		if (acceptStage != null) {
			acceptStage.post(null);
			acceptStage = null;
		}
		SettableStage<RawCommit> pendingStage = new SettableStage<>();
		pendingStages.put(commitId, pendingStage);
		return pendingStage;
	}
}
