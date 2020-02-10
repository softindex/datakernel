package io.datakernel.ot;

import io.datakernel.common.exception.UncheckedException;
import io.datakernel.ot.OTUplinkStorage.Storage.SyncData;
import io.datakernel.ot.exceptions.OTTransformException;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.common.collection.CollectionUtils.concat;
import static io.datakernel.promise.Promises.retry;
import static java.util.Collections.emptyList;

@SuppressWarnings("WeakerAccess")
public final class OTUplinkStorage<K, D> implements OTUplink<Long, D, OTUplinkStorage.ProtoCommit<D>> {

	public static final long FIRST_COMMIT_ID = 1L;
	public static final int NO_LEVEL = 0;

	public static final class ProtoCommit<D> {
		private final long id;
		private final List<D> diffs;

		private ProtoCommit(long id, List<D> diffs) {
			this.id = id;
			this.diffs = diffs;
		}

		public long getId() {
			return id;
		}

		public List<D> getDiffs() {
			return diffs;
		}
	}

	public interface Storage<K, D> {
		Promise<Boolean> init(long commitId, List<D> snapshot, K uplinkCommitId, long uplinkLevel);

		Promise<@Nullable FetchData<Long, D>> getSnapshot();

		Promise<Long> getHead();

		default Promise<FetchData<Long, D>> fetch(long commitId) {
			List<D> diffs = new ArrayList<>();
			return getHead()
					.then(headCommitId ->
							Promises.loop(commitId + 1L,
									i -> i <= headCommitId,
									i -> getCommit(i)
											.map(commit -> {
												diffs.addAll(commit.getDiffs());
												return i + 1;
											}))
									.map($ -> new FetchData<>(headCommitId, NO_LEVEL, diffs)));
		}

		default Promise<FetchData<Long, D>> poll(long currentCommitId) {
			return fetch(currentCommitId);
		}

		Promise<ProtoCommit<D>> getCommit(long commitId);

		Promise<Boolean> add(long commitId, List<D> diffs);

		final class SyncData<K, D> {
			private final long commitId;
			private final K uplinkCommitId;
			private final long uplinkLevel;
			private final List<D> uplinkDiffs;
			@Nullable
			private final Object protoCommit;

			public SyncData(long id, K uplinkCommitId, long uplinkLevel, List<D> uplinkDiffs, @Nullable Object protoCommit) {
				this.commitId = id;
				this.uplinkCommitId = uplinkCommitId;
				this.uplinkLevel = uplinkLevel;
				this.uplinkDiffs = uplinkDiffs;
				this.protoCommit = protoCommit;
			}

			public long getCommitId() {
				return commitId;
			}

			public K getUplinkCommitId() {
				return uplinkCommitId;
			}

			public long getUplinkLevel() {
				return uplinkLevel;
			}

			public List<D> getUplinkDiffs() {
				return uplinkDiffs;
			}

			@Nullable
			public Object getProtoCommit() {
				return protoCommit;
			}

			public boolean isSyncing() {
				return protoCommit != null;
			}
		}

		Promise<SyncData<K, D>> getSyncData();

		default Promise<Boolean> isSyncing() {
			return getSyncData().map(SyncData::isSyncing);
		}

		Promise<Void> startSync(long headCommitId, K uplinkCommitId, Object protoCommit);

		Promise<Boolean> completeSync(long commitId, List<D> diffs, K uplinkCommitId, long uplinkLevel, List<D> uplinkDiffs);
	}

	private final Storage<K, D> storage;

	private final OTSystem<D> otSystem;
	private final OTUplink<K, D, Object> uplink;

	private OTUplinkStorage(Storage<K, D> storage, OTSystem<D> otSystem, OTUplink<K, D, ?> uplink) {
		this.otSystem = otSystem;
		this.storage = storage;
		//noinspection unchecked
		this.uplink = (OTUplink<K, D, Object>) uplink;
	}

	public Promise<Void> sync() {
		return startSync()
				.then(syncData -> uplink.push(syncData.getProtoCommit())
						.then(uplinkFetchedData -> Promise.ofCallback(cb ->
								completeSync(syncData.getCommitId(), new ArrayList<>(), uplinkFetchedData.getCommitId(), uplinkFetchedData.getLevel(), uplinkFetchedData.getDiffs(), cb))));
	}

	Promise<SyncData<K, D>> startSync() {
		return storage.getSyncData()
				.then(syncData -> {
					if (syncData.getProtoCommit() != null) {
						return Promise.of(syncData);
					}
					return storage.fetch(syncData.getCommitId())
							.then(fetchedData -> {
								long headCommitId = fetchedData.getCommitId();
								List<D> diffs = fetchedData.getDiffs();
								return uplink.createProtoCommit(syncData.getUplinkCommitId(), concat(syncData.getUplinkDiffs(), diffs), 0)
										.then(protoCommit ->
												storage.startSync(headCommitId, syncData.getUplinkCommitId(), protoCommit)
														.map($ -> new SyncData<>(headCommitId, syncData.getUplinkCommitId(), syncData.getUplinkLevel(), diffs, protoCommit)));
							});
				});
	}

	void completeSync(long commitId, List<D> accumulatedDiffs, K uplinkCommitId, long uplinkLevel, List<D> uplinkDiffs, SettablePromise<Void> cb) {
		storage.fetch(commitId)
				.whenResult(fetchData -> {
					TransformResult<D> transformResult;
					try {
						transformResult = otSystem.transform(uplinkDiffs, fetchData.getDiffs());
					} catch (OTTransformException e) {
						throw new UncheckedException(e);
					}

					accumulatedDiffs.addAll(transformResult.left);
					storage.completeSync(fetchData.getCommitId(), accumulatedDiffs, uplinkCommitId, uplinkLevel, transformResult.right)
							.whenResult(ok -> {
								if (ok) {
									cb.set(null);
								} else {
									completeSync(commitId, accumulatedDiffs, uplinkCommitId, uplinkLevel, transformResult.right, cb);
								}
							})
							.whenException(cb::setException);
				})
				.whenException(cb::setException);
	}

	@Override
	public Promise<FetchData<Long, D>> checkout() {
		return retry(
				() -> storage.getSnapshot()
						.then(snapshotData -> snapshotData != null ?
								Promise.of(snapshotData) :
								uplink.checkout()
										.then(uplinkSnapshotData -> storage.init(FIRST_COMMIT_ID, uplinkSnapshotData.getDiffs(), uplinkSnapshotData.getCommitId(), uplinkSnapshotData.getLevel())
												.then(ok -> Promise.of(ok ?
														new FetchData<>(FIRST_COMMIT_ID, NO_LEVEL, uplinkSnapshotData.getDiffs()) :
														null)))),
				(v, e) -> e != null || v != null)
				.then(snapshotData ->
						storage.fetch(snapshotData.getCommitId())
								.map(fetchData ->
										new FetchData<>(fetchData.getCommitId(), NO_LEVEL, concat(snapshotData.getDiffs(), fetchData.getDiffs()))));
	}

	@Override
	public Promise<ProtoCommit<D>> createProtoCommit(Long parentCommitId, List<D> diffs, long parentLevel) {
		return Promise.of(new ProtoCommit<>(parentCommitId, diffs));
	}

	@Override
	public Promise<FetchData<Long, D>> push(ProtoCommit<D> protoCommit) {
		return Promise.ofCallback(cb -> doPush(protoCommit.getId(), protoCommit.getDiffs(), emptyList(), cb));
	}

	void doPush(long commitId, List<D> diffs, List<D> fetchedDiffs, SettablePromise<FetchData<Long, D>> cb) {
		storage.add(commitId, diffs)
				.whenResult(ok -> {
					if (ok) {
						cb.set(new FetchData<>(commitId + 1, NO_LEVEL, fetchedDiffs));
					} else {
						storage.fetch(commitId)
								.whenResult(fetchData -> {
									TransformResult<D> transformResult;
									try {
										transformResult = otSystem.transform(fetchData.getDiffs(), diffs);
									} catch (OTTransformException e) {
										throw new UncheckedException(e);
									}
									doPush(fetchData.getCommitId(), transformResult.left, concat(fetchedDiffs, transformResult.right), cb);
								})
								.whenException(cb::setException);
					}
				})
				.whenException(cb::setException);
	}

	@Override
	public Promise<FetchData<Long, D>> fetch(Long currentCommitId) {
		return storage.fetch(currentCommitId);
	}

	@Override
	public Promise<FetchData<Long, D>> poll(Long currentCommitId) {
		return storage.poll(currentCommitId);
	}

}
