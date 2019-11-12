package io.global.ot.server;

import io.datakernel.async.service.EventloopService;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.global.common.SignedData;
import io.global.ot.api.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.thisMethod;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.codec.StructuredCodecs.BYTE_CODEC;
import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static io.global.util.Utils.arrayStartsWith;
import static java.util.Arrays.asList;

public final class CommitStorageRocksDb implements CommitStorage, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(CommitStorageRocksDb.class);

	private static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);
	private static final StructuredCodec<RawCommit> RAW_COMMIT_CODEC = REGISTRY.get(RawCommit.class);
	private static final StructuredCodec<RepoID> REPO_ID_CODEC = REGISTRY.get(RepoID.class);
	private static final StructuredCodec<Tuple2<RepoID, CommitId>> REPO_COMMIT_CODEC = REGISTRY.get(new TypeT<Tuple2<RepoID, CommitId>>() {});
	private static final StructuredCodec<Tuple2<CommitId, CommitId>> COMMIT_COMMIT_CODEC = REGISTRY.get(new TypeT<Tuple2<CommitId, CommitId>>() {});
	private static final StructuredCodec<Tuple2<RepoID, SignedData<RawPullRequest>>> REPO_PULL_REQUESTS_CODEC = REGISTRY.get(new TypeT<Tuple2<RepoID,
			SignedData<RawPullRequest>>>() {});
	private static final StructuredCodec<SignedData<RawSnapshot>> SIGNED_SNAPSHOT_CODEC = REGISTRY.get(new TypeT<SignedData<RawSnapshot>>() {});
	private static final StructuredCodec<SignedData<RawCommitHead>> SIGNED_HEAD_CODEC = REGISTRY.get(new TypeT<SignedData<RawCommitHead>>() {});
	private static final byte EMPTY = 0;

	private final Executor executor;
	private final Eventloop eventloop;
	private final String storagePath;
	private final DBOptions dbOptions;
	private final TransactionDBOptions transactionDBOptions;
	private final ColumnFamilyOptions columnFamilyOptions;
	private final WriteOptions writeOptions;
	private final FlushOptions flushOptions;
	private final List<ColumnFamilyHandle> handles = new ArrayList<>();

	private TransactionDB db;

	// columns
	private Column<CommitId, RawCommit> commits;
	private Column<Tuple2<RepoID, CommitId>, SignedData<RawSnapshot>> snapshots;
	private Column<Tuple2<RepoID, CommitId>, SignedData<RawCommitHead>> heads;
	private Column<Tuple2<RepoID, SignedData<RawPullRequest>>, Byte> pullRequests;
	private Column<CommitId, Integer> incompleteParentsCount;
	private Column<Tuple2<CommitId, CommitId>, Byte> parentToChildren;
	private Column<CommitId, Byte> pendingCommits;

	private CommitStorageRocksDb(Executor executor, Eventloop eventloop, String storagePath, DBOptions dbOptions, TransactionDBOptions transactionDBOptions,
								 ColumnFamilyOptions columnFamilyOptions, WriteOptions writeOptions, FlushOptions flushOptions) {
		this.executor = executor;
		this.eventloop = eventloop;
		this.dbOptions = dbOptions;
		this.transactionDBOptions = transactionDBOptions;
		this.columnFamilyOptions = columnFamilyOptions;
		this.writeOptions = writeOptions;
		this.flushOptions = flushOptions;
		this.storagePath = storagePath;
	}

	public static CommitStorageRocksDb create(Executor executor, Eventloop eventloop, String storagePath, DBOptions dbOptions,
											  TransactionDBOptions transactionDBOptions, ColumnFamilyOptions columnFamilyOptions, WriteOptions writeOptions,
											  FlushOptions flushOptions) {
		return new CommitStorageRocksDb(executor, eventloop, storagePath, dbOptions, transactionDBOptions, columnFamilyOptions, writeOptions, flushOptions);
	}

	public static CommitStorageRocksDb create(Executor executor, Eventloop eventloop, String storagePath) {
		DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
		TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
		return new CommitStorageRocksDb(executor, eventloop, storagePath, dbOptions, transactionDBOptions,
				new ColumnFamilyOptions(), new WriteOptions(), new FlushOptions());
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try {
						initDb();
					} catch (RocksDBException e) {
						throw new UncheckedException(e);
					}
				});
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try {
						db.flush(flushOptions, commits.handle);
						db.flush(flushOptions, snapshots.handle);
						db.flush(flushOptions, heads.handle);
						db.flush(flushOptions, pullRequests.handle);
						db.flush(flushOptions, incompleteParentsCount.handle);
						db.flush(flushOptions, parentToChildren.handle);
						db.flush(flushOptions, pendingCommits.handle);
					} catch (RocksDBException e) {
						throw new UncheckedException(e);
					}
					for (ColumnFamilyHandle handle : handles) {
						handle.close();
					}

					db.close();
				});
	}

	private void initDb() throws RocksDBException {
		List<ColumnFamilyDescriptor> descriptors = asList(
				new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
				new ColumnFamilyDescriptor("commits".getBytes(), columnFamilyOptions),
				new ColumnFamilyDescriptor("snapshots".getBytes(), columnFamilyOptions),
				new ColumnFamilyDescriptor("heads".getBytes(), columnFamilyOptions),
				new ColumnFamilyDescriptor("pull requests".getBytes(), columnFamilyOptions),
				new ColumnFamilyDescriptor("incomplete parents".getBytes(), columnFamilyOptions),
				new ColumnFamilyDescriptor("parent to children".getBytes(), columnFamilyOptions),
				new ColumnFamilyDescriptor("pending commits".getBytes(), columnFamilyOptions)
		);
		try {
			Files.createDirectories(Paths.get(storagePath).getParent());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		db = TransactionDB.open(dbOptions, transactionDBOptions, storagePath, descriptors, handles);
		commits = new Column<>(handles.get(1), COMMIT_ID_CODEC, RAW_COMMIT_CODEC);
		snapshots = new Column<>(handles.get(2), REPO_COMMIT_CODEC, SIGNED_SNAPSHOT_CODEC);
		heads = new Column<>(handles.get(3), REPO_COMMIT_CODEC, SIGNED_HEAD_CODEC);
		pullRequests = new Column<>(handles.get(4), REPO_PULL_REQUESTS_CODEC, BYTE_CODEC);
		incompleteParentsCount = new Column<>(handles.get(5), COMMIT_ID_CODEC, INT_CODEC);
		parentToChildren = new Column<>(handles.get(6), COMMIT_COMMIT_CODEC, BYTE_CODEC);
		pendingCommits = new Column<>(handles.get(7), COMMIT_ID_CODEC, BYTE_CODEC);
	}

	@Override
	public Promise<Map<CommitId, SignedData<RawCommitHead>>> getHeads(RepoID repositoryId) {
		return Promise.ofBlockingCallable(executor, () -> getRange(heads, repositoryId, REPO_ID_CODEC, Tuple2::getValue2))
				.whenComplete(toLogger(logger, TRACE, thisMethod(), repositoryId, this));
	}

	@Override
	public Promise<Void> updateHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads, Set<CommitId> excludedHeads) {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try (WriteBatch batch = new WriteBatch()) {
						for (SignedData<RawCommitHead> head : newHeads) {
							Tuple2<RepoID, CommitId> key = new Tuple2<>(repositoryId, head.getValue().getCommitId());
							byte[] keyBytes = encodeAsArray(heads.keyCodec, key);
							byte[] valueBytes = encodeAsArray(heads.valueCodec, head);
							batch.put(heads.handle, keyBytes, valueBytes);
						}
						for (CommitId commitId : excludedHeads) {
							byte[] keyBytes = encodeAsArray(heads.keyCodec, new Tuple2<>(repositoryId, commitId));
							batch.delete(heads.handle, keyBytes);
						}
						db.write(writeOptions, batch);
					} catch (RocksDBException e) {
						throw new UncheckedException(e);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), repositoryId, newHeads, excludedHeads, this));
	}

	@Override
	public Promise<Boolean> hasCommit(CommitId commitId) {
		return Promise.ofBlockingCallable(executor, () -> get(commits, commitId) != null)
				.whenComplete(toLogger(logger, TRACE, thisMethod(), commitId, this));
	}

	@Override
	public Promise<Optional<RawCommit>> loadCommit(CommitId commitId) {
		return Promise.ofBlockingCallable(executor, () -> Optional.ofNullable(get(commits, commitId)))
				.whenComplete(toLogger(logger, TRACE, thisMethod(), commitId, this));
	}

	@Override
	public Promise<Set<CommitId>> getChildren(CommitId commitId) {
		return Promise.ofBlockingCallable(executor, () -> getRange(parentToChildren, commitId, COMMIT_ID_CODEC, Tuple2::getValue2).keySet())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), commitId, this));
	}

	@Override
	public Promise<Boolean> saveCommit(CommitId commitId, RawCommit rawCommit) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Transaction txn = db.beginTransaction(writeOptions); ReadOptions ro = new ReadOptions().setSnapshot(txn.getSnapshot())) {
						RawCommit old = get(txn, ro, commits, commitId);
						put(txn, commits, commitId, rawCommit);
						if (old != null) {
							txn.commit();
							return false;
						}
						if (rawCommit.getParents().isEmpty()) {
							put(txn, pendingCommits, commitId, EMPTY);
							txn.commit();
							return true;
						}
						for (CommitId parentId : rawCommit.getParents()) {
							put(txn, parentToChildren, new Tuple2<>(parentId, commitId), EMPTY);
						}
						int incompleteCount = 0;
						for (CommitId parentId : rawCommit.getParents()) {
							if (parentId.isRoot()) {
								continue;
							}
							Integer incompleteParents = get(txn, ro, incompleteParentsCount, parentId);
							if ((incompleteParents != null && incompleteParents != 0) || get(txn, ro, commits, parentId) == null) {
								incompleteCount++;
							}
						}
						if (incompleteCount == 0) {
							put(txn, pendingCommits, commitId, EMPTY);
						} else {
							put(txn, incompleteParentsCount, commitId, incompleteCount);
						}
						txn.commit();
						return true;
					}
				})
				.whenComplete(toLogger(logger, TRACE, thisMethod(), commitId, rawCommit, this));
	}

	@Override
	public Promise<Boolean> saveSnapshot(SignedData<RawSnapshot> encryptedSnapshot) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					RawSnapshot rawSnapshot = encryptedSnapshot.getValue();
					Tuple2<RepoID, CommitId> entry = new Tuple2<>(rawSnapshot.getRepositoryId(), rawSnapshot.getCommitId());
					SignedData<RawSnapshot> old = get(snapshots, entry);
					put(snapshots, entry, encryptedSnapshot);
					db.flush(flushOptions, snapshots.handle);
					return old == null;
				})
				.whenComplete(toLogger(logger, thisMethod(), encryptedSnapshot, this));
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
		return Promise.ofBlockingCallable(executor, () -> Optional.ofNullable(get(snapshots, new Tuple2<>(repositoryId, commitId))))
				.whenComplete(toLogger(logger, TRACE, thisMethod(), repositoryId, commitId, this));
	}

	@Override
	public Promise<Set<CommitId>> listSnapshotIds(RepoID repositoryId) {
		return Promise.ofBlockingCallable(executor, () -> getRange(snapshots, repositoryId, REPO_ID_CODEC, Tuple2::getValue2).keySet())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), repositoryId, this));
	}

	@Override
	public Promise<Boolean> savePullRequest(SignedData<RawPullRequest> pullRequest) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					Tuple2<RepoID, SignedData<RawPullRequest>> key = new Tuple2<>(pullRequest.getValue().getRepository(), pullRequest);
					Byte b = get(pullRequests, key);
					put(pullRequests, key, EMPTY);
					return b == null;
				})
				.whenComplete(toLogger(logger, thisMethod(), pullRequest, this));
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repository) {
		return Promise.ofBlockingCallable(executor, () -> getRange(pullRequests, repository, REPO_ID_CODEC, Tuple2::getValue2).keySet())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), repository, this));
	}

	@Override
	public Promise<Void> markCompleteCommits() {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try (Transaction txn = db.beginTransaction(writeOptions)) {
						ReadOptions ro = new ReadOptions().setSnapshot(txn.getSnapshot());
						List<CommitId> completeCommits = new ArrayList<>();
						try (RocksIterator it = txn.getIterator(ro, pendingCommits.handle)) {
							for (it.seekToFirst(); it.isValid(); it.next()) {
								completeCommits.add(decode(COMMIT_ID_CODEC, it.key()));
								txn.delete(pendingCommits.handle, it.key());
							}
						}

						while (!completeCommits.isEmpty()) {
							List<CommitId> nextOnes = new ArrayList<>();
							for (CommitId completeCommit : completeCommits) {
								for (CommitId childId : getRange(txn, ro, parentToChildren, completeCommit, COMMIT_ID_CODEC, Tuple2::getValue2).keySet()) {
									Integer count = get(txn, ro, incompleteParentsCount, childId);
									if (count != null) {
										if (--count == 0) {
											nextOnes.add(childId);
											byte[] childKey = encodeAsArray(COMMIT_ID_CODEC, childId);
											txn.delete(incompleteParentsCount.handle, childKey);
										} else {
											put(txn, incompleteParentsCount, childId, count);
										}
									}
								}
							}
							completeCommits = nextOnes;
						}
						txn.commit();
					} catch (RocksDBException | ParseException e) {
						throw new UncheckedException(e);
					}
				})
				.whenComplete(toLogger(logger, TRACE, thisMethod(), this));
	}

	@Override
	public Promise<Boolean> isCompleteCommit(CommitId commitId) {
		if (commitId.isRoot()) return Promise.of(true);

		return Promise.ofBlockingCallable(executor,
				() -> {
					Integer count = get(incompleteParentsCount, commitId);
					RawCommit commit = get(commits, commitId);
					return commit != null && (count == null || count == 0);
				})
				.whenComplete(toLogger(logger, TRACE, thisMethod(), commitId, this));
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	// region helpers
	private <K, V> void put(Column<K, V> column, K key, V value) throws RocksDBException {
		put(null, column, key, value);
	}

	private <K, V> void put(@Nullable Transaction tnx, Column<K, V> column, K key, V value) throws RocksDBException {
		byte[] keyBytes = encodeAsArray(column.keyCodec, key);
		byte[] valueBytes = encodeAsArray(column.valueCodec, value);
		if (tnx == null) {
			db.put(column.handle, writeOptions, keyBytes, valueBytes);
		} else {
			tnx.put(column.handle, keyBytes, valueBytes);
		}
	}

	@Nullable
	private <K, V> V get(Column<K, V> column, K key) throws RocksDBException, ParseException {
		return get(null, null, column, key);
	}

	@Nullable
	private <K, V> V get(@Nullable Transaction tnx, @Nullable ReadOptions ro, Column<K, V> column, K key) throws RocksDBException, ParseException {
		byte[] keyBytes = encodeAsArray(column.keyCodec, key);
		byte[] valueBytes = tnx == null ?
				db.get(column.handle, keyBytes) :
				tnx.get(column.handle, ro, keyBytes);
		return valueBytes == null ? null : decode(column.valueCodec, valueBytes);
	}

	private <K, V, P, K2> Map<K2, V> getRange(Column<K, V> column, P prefix, StructuredCodec<P> prefixCodec, Function<K, K2> keyMapper) throws ParseException {
		return getRange(null, null, column, prefix, prefixCodec, keyMapper);
	}

	private <K, V, P, K2> Map<K2, V> getRange(@Nullable Transaction tnx, @Nullable ReadOptions ro, Column<K, V> column, P prefix,
											  StructuredCodec<P> prefixCodec, Function<K, K2> keyMapper) throws ParseException {
		byte[] prefixBytes = encodeAsArray(prefixCodec, prefix);
		Map<K2, V> result = new HashMap<>();

		try (RocksIterator iterator = tnx == null ?
				db.newIterator(column.handle) :
				tnx.getIterator(ro, column.handle)
		) {
			for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
				byte[] keyBytes = iterator.key();
				if (!arrayStartsWith(keyBytes, prefixBytes)) {
					break;
				}

				result.put(keyMapper.apply(decode(column.keyCodec, keyBytes)), decode(column.valueCodec, iterator.value()));
			}
		}
		return result;
	}

	private static class Column<K, V> {
		private final ColumnFamilyHandle handle;
		private final StructuredCodec<K> keyCodec;
		private final StructuredCodec<V> valueCodec;

		Column(ColumnFamilyHandle handle, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
			this.handle = handle;
			this.keyCodec = keyCodec;
			this.valueCodec = valueCodec;
		}
	}
	// endregion

	@Override
	public String toString() {
		return "CommitStorageRocksDb{" +
				"storagePath='" + storagePath + '\'' +
				'}';
	}
}
