package io.global.ot.util;

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.datakernel.util.Tuple2;
import io.global.common.SignedData;
import io.global.ot.api.CommitEntry;
import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;
import io.global.ot.api.RawCommitHead;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiFunction;

import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.ot.util.HttpDataFormats.COMMIT_CODEC;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toSet;

public class RawCommitChannels {
	public static final StacklessException UNEXPECTED_END_OF_STREAM = new StacklessException(RawCommitChannels.class, "Unexpected end of stream");
	public static final StacklessException UNEXPECTED_COMMIT = new StacklessException(RawCommitChannels.class, "Unexpected commit");
	public static final StacklessException COMMIT_ID_EXCEPTION = new StacklessException(RawCommitChannels.class, "Commit id does not match commit");

	private RawCommitChannels() {
		throw new AssertionError();
	}

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
											if (tuple == null) {
												return Promise.of(null);
											}
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

	public static BiFunction<Integer, CommitId, Promise<RawCommit>> commitLoader(ChannelSupplier<CommitEntry> commitChannel) {
		return (level, commitId) -> Promises.until(
				commitChannel::get,
				rawCommitEntry -> {
					if (rawCommitEntry == null) {
						return Promise.ofException(new StacklessException("Incorrect commit stream"));
					}
					int compare = commitId.compareTo(rawCommitEntry.getCommitId());
					if (compare < 0) {
						return Promise.ofException(new StacklessException("Incorrect commit stream"));
					}
					return Promise.of(compare == 0);
				})
				.map(CommitEntry::getCommit);
	}

	public static ChannelSupplier<CommitEntry> validateStream(ChannelSupplier<CommitEntry> channel) {
		return new AbstractChannelSupplier<CommitEntry>(channel) {
			CommitId lastCommitId;

			@Override
			protected @NotNull Promise<CommitEntry> doGet() {
				return channel.get()
						.thenEx(this::sanitize)
						.then(rawCommitEntry -> {
							if (rawCommitEntry == null) {
								return Promise.of(null);
							}
							if (lastCommitId != null) {
								int compare = lastCommitId.compareTo(rawCommitEntry.getCommitId());
								if (compare > 0) {
									return Promise.ofException(new StacklessException("Incorrect commit stream"));
								}
							}
							lastCommitId = rawCommitEntry.getCommitId();
							return Promise.of(rawCommitEntry);
						});
			}
		};
	}

	public static ChannelSupplier<CommitEntry> validateSupplier(ChannelSupplier<CommitEntry> commitChannel, Set<CommitId> heads) {
		StreamValidator validator = new StreamValidator(heads);

		return new AbstractChannelSupplier<CommitEntry>(commitChannel) {
			@Override
			protected Promise<CommitEntry> doGet() {
				return commitChannel.get()
						.thenEx(this::sanitize)
						.then(validator::validateCommit);
			}
		};
	}

	public static ChannelConsumer<CommitEntry> validateConsumer(ChannelConsumer<CommitEntry> commitChannel, Set<SignedData<RawCommitHead>> heads) {
		StreamValidator streamValidator = new StreamValidator(heads.stream()
				.map(SignedData::getValue)
				.map(RawCommitHead::getCommitId)
				.collect(toSet()));

		return new AbstractChannelConsumer<CommitEntry>(commitChannel) {
			@Override
			protected Promise<Void> doAccept(@Nullable CommitEntry entry) {
				return streamValidator.validateCommit(entry)
						.then(commitChannel::accept)
						.thenEx(this::sanitize);
			}
		};
	}

	private static class StreamValidator {
		private final PriorityQueue<CommitId> queue;

		private StreamValidator(Set<CommitId> startNodes) {
			this.queue = new PriorityQueue<>(startNodes);
		}

		private Promise<CommitEntry> validateCommit(@Nullable CommitEntry entry) {
			CommitId expected = queue.poll();
			if (entry == null && expected != null) {
				return Promise.ofException(UNEXPECTED_END_OF_STREAM);
			}
			if (entry != null && expected == null) {
				return Promise.ofException(UNEXPECTED_COMMIT);
			}
			if (entry == null) {
				return Promise.of(null);
			}
			if (!entry.getCommitId().equals(expected)) {
				return Promise.ofException(UNEXPECTED_COMMIT);
			}
			if (!CommitId.ofCommitData(entry.getLevel(), encodeAsArray(COMMIT_CODEC, entry.getCommit())).equals(expected)) {
				return Promise.ofException(COMMIT_ID_EXCEPTION);
			}

			for (CommitId parent : entry.getCommit().getParents()) {
				if (!queue.contains(parent) && !parent.isRoot()) {
					queue.add(parent);
				}
			}

			return Promise.of(entry);
		}
	}
}
