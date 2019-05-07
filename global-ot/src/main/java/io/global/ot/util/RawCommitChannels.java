package io.global.ot.util;

import io.datakernel.async.AsyncFunction2;
import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitEntry;
import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

import static java.util.Comparator.comparing;

public class RawCommitChannels {
	private RawCommitChannels() {}

	public static ChannelSupplier<CommitEntry> mergeChannels(List<ChannelSupplier<CommitEntry>> commitChannels) {
		PriorityQueue<Tuple2<CommitEntry, ChannelSupplier<CommitEntry>>> queue = new PriorityQueue<>(comparing(Tuple2::getValue1));
		return ChannelSupplier.ofPromise(
				Promises.all(commitChannels.stream()
						.map(channel -> channel.get()
								.whenResult(rawCommitEntry -> {
									if (rawCommitEntry != null) {
										queue.add(new Tuple2<>(rawCommitEntry, channel));
									}
								})))
						.map($ -> new AbstractChannelSupplier<CommitEntry>() {
							CommitId lastCommitId;

							@Override
							protected @NotNull Promise<CommitEntry> doGet() {
								return Promises.until(
										() -> {
											Tuple2<CommitEntry, ChannelSupplier<CommitEntry>> tuple = queue.peek();
											if (tuple == null) return Promise.of(null);
											return tuple.getValue2()
													.get()
													.thenEx(this::sanitize)
													.whenResult(rawCommitEntry -> {
														queue.poll();
														if (rawCommitEntry != null) {
															queue.add(new Tuple2<>(rawCommitEntry, tuple.getValue2()));
														}
													})
													.map($ -> tuple.getValue1());
										},
										AsyncPredicate.of(result -> result == null || !Objects.equals(lastCommitId, result.getCommitId())))
										.whenResult(result -> this.lastCommitId = result.getCommitId());
							}

							@Override
							protected void onClosed(@NotNull Throwable e) {
								queue.forEach(tuple -> tuple.getValue2().close(e));
								queue.clear();
							}
						}));
	}

	public static AsyncFunction2<Integer, CommitId, RawCommit> commitLoader(ChannelSupplier<CommitEntry> commitChannel) {
		return (level, commitId) -> Promises.until(
				commitChannel::get,
				rawCommitEntry -> {
					if (rawCommitEntry == null) return Promise.ofException(new StacklessException());
					int compare = CommitIdWithLevel.compare(level, commitId, rawCommitEntry.getCommit().getLevel(), rawCommitEntry.getCommitId());
					if (compare < 0) return Promise.ofException(new StacklessException());
					return Promise.of(compare == 0);
				})
				.map(CommitEntry::getCommit);
	}

	public static ChannelSupplier<CommitEntry> validateStream(ChannelSupplier<CommitEntry> channel) {
		return new AbstractChannelSupplier<CommitEntry>(channel) {
			long lastLevel;
			CommitId lastCommitId;

			@Override
			protected @NotNull Promise<CommitEntry> doGet() {
				return channel.get()
						.thenEx(this::sanitize)
						.then(rawCommitEntry -> {
							if (rawCommitEntry == null) return Promise.of(null);
							if (lastCommitId != null) {
								int compare = CommitIdWithLevel.compare(lastLevel, lastCommitId, rawCommitEntry.getLevel(), rawCommitEntry.getCommitId());
								if (compare > 0) {
									return Promise.ofException(new StacklessException());
								}
							}
							lastCommitId = rawCommitEntry.getCommitId();
							return Promise.of(rawCommitEntry);
						});
			}
		};
	}

	public static ChannelSupplier<CommitEntry> validateStream(ChannelSupplier<CommitEntry> commitChannel, Map<CommitId, Long> heads) {
		PriorityQueue<CommitIdWithLevel> queue = new PriorityQueue<>();
		for (Map.Entry<CommitId, Long> entry : heads.entrySet()) {
			queue.add(new CommitIdWithLevel(entry.getValue(), entry.getKey()));
		}

		return new AbstractChannelSupplier<CommitEntry>(commitChannel) {
			@Override
			protected Promise<CommitEntry> doGet() {
				return commitChannel.get()
						.thenEx(this::sanitize)
						.then(entry -> {
							CommitIdWithLevel expected = queue.poll();
							if (entry == null && expected != null) {
								return Promise.ofException(new StacklessException());
							}
							if (entry != null && expected == null) {
								return Promise.ofException(new StacklessException());
							}
							if (entry == null) {
								return Promise.of(null);
							}
							if (entry.getLevel() != expected.getLevel() || !Objects.equals(entry.getCommitId(), expected.getCommitId())) {
								return Promise.ofException(new StacklessException());
							}

							for (Map.Entry<CommitId, Long> parentEntry : entry.getCommit().getParentLevels().entrySet()) {
								CommitIdWithLevel item = new CommitIdWithLevel(parentEntry.getValue(), parentEntry.getKey());
								if (!queue.contains(item)) {
									queue.add(item);
								}
							}

							return Promise.of(entry);
						});
			}
		};
	}

}
