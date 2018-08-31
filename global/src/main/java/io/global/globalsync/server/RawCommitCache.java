package io.global.globalsync.server;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.exception.ToDoException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.global.globalsync.api.CommitId;
import io.global.globalsync.api.RawCommit;

import java.util.HashMap;
import java.util.Map;

final class RawCommitCache implements AsyncConsumer<RawCommitEntry> {
	private final Map<CommitId, RawCommit> preloadedCommits = new HashMap<>();
	private final Map<CommitId, SettableStage<RawCommit>> pendingStages = new HashMap<>();
	private SettableStage<Void> acceptStage;

	public RawCommitCache() {
	}

	public static RawCommitCache of(StreamProducer<RawCommitEntry> streamProducer) {
		RawCommitCache streamingCache = new RawCommitCache();
		streamProducer.streamTo(StreamConsumer.ofSerialConsumer(SerialConsumer.of(streamingCache)))
				.getEndOfStream()
				.thenRun(streamingCache::onEndOfStream)
				.whenException(streamingCache::onError);
		return streamingCache;
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
