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

package io.global.ot.client;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTCommitFactory.DiffsWithLevel;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.SimKey;
import io.global.common.api.EncryptedData;
import io.global.ot.api.*;
import org.jetbrains.annotations.NotNull;
import org.spongycastle.crypto.CryptoException;

import java.util.*;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.datakernel.common.collection.CollectionUtils.*;
import static io.datakernel.ot.OTAlgorithms.excludeParents;
import static io.datakernel.ot.OTAlgorithms.merge;
import static io.global.common.CryptoUtils.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.*;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class OTDriver {
	private static final StructuredCodec<RawCommit> COMMIT_CODEC = REGISTRY.get(RawCommit.class);
	private static final StructuredCodec<List<byte[]>> COMMIT_DIFFS_CODEC = REGISTRY.get(new TypeT<List<byte[]>>() {});
	private static final StructuredCodec<RawCommitHead> COMMIT_HEAD_CODEC = REGISTRY.get(RawCommitHead.class);
	private static final StructuredCodec<RawSnapshot> SNAPSHOT_CODEC = REGISTRY.get(new TypeT<RawSnapshot>() {});

	private final GlobalOTNode service;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private Map<Hash, SimKey> simKeys = new HashMap<>();

	@NotNull
	private SimKey currentSimKey;

	public OTDriver(GlobalOTNode service, @NotNull SimKey currentSimKey) {
		this.service = service;
		this.currentSimKey = currentSimKey;
		simKeys.put(Hash.sha1(currentSimKey.getBytes()), currentSimKey);
	}

	@NotNull
	public SimKey getCurrentSimKey() {
		return currentSimKey;
	}

	public <D> OTCommit<CommitId, D> createCommit(int epoch, MyRepositoryId<D> myRepositoryId,
			Map<CommitId, DiffsWithLevel<D>> parentDiffs) {
		long timestamp = now.currentTimeMillis();
		EncryptedData encryptedDiffs = encryptAES(
				encodeAsArray(COMMIT_DIFFS_CODEC,
						parentDiffs.values()
								.stream()
								.map(value -> encodeAsArray(myRepositoryId.getDiffsCodec(), value.getDiffs()))
								.collect(toList())),
				currentSimKey.getAesKey());
		byte[] rawCommitBytes = encodeAsArray(COMMIT_CODEC,
				RawCommit.of(epoch,
						parentDiffs.keySet(),
						encryptedDiffs,
						Hash.sha1(currentSimKey.getAesKey().getKey()),
						timestamp));
		long level = parentDiffs.values().stream().map(DiffsWithLevel::getLevel).max(naturalOrder()).orElse(0L) + 1L;
		CommitId commitId = CommitId.of(level, sha256(rawCommitBytes));
		return OTCommit.of(epoch, commitId, parentDiffs)
				.withTimestamp(timestamp)
				.withSerializedData(rawCommitBytes);
	}

	public Promise<Optional<SimKey>> getSharedKey(MyRepositoryId<?> myRepositoryId,
			PubKey senderPubKey, Hash simKeyHash) {
		return service.getSharedKey(myRepositoryId.getRepositoryId().getOwner(), simKeyHash)
				.mapEx((signedSimKey, e) -> {
					if (e == null) {
						if (!signedSimKey.verify(myRepositoryId.getRepositoryId().getOwner())) {
							return Optional.empty();
						}

						SimKey simKey;
						try {
							simKey = signedSimKey.getValue().decryptSimKey(myRepositoryId.getPrivKey());
						} catch (CryptoException ignored) {
							return Optional.empty();
						}

						if (!Arrays.equals(sha1(simKey.getBytes()), simKeyHash.getBytes())) {
							return Optional.empty();
						}

						simKeys.put(simKeyHash, simKey);
						return Optional.of(simKey);
					} else {
						return Optional.empty();
					}
				});
	}

	public Promise<SimKey> ensureSimKey(MyRepositoryId<?> myRepositoryId,
			Set<RepoID> originRepositoryIds, Hash simKeyHash) {
		return simKeys.containsKey(simKeyHash) ?
				Promise.of(simKeys.get(simKeyHash)) :
				Promises.any(union(singleton(myRepositoryId.getRepositoryId()), originRepositoryIds).stream()
						.map(RepoID::getOwner)
						.collect(toSet())
						.stream()
						.map(originPubKey -> getSharedKey(myRepositoryId, originPubKey, simKeyHash)
								.then(Promise::ofOptional)));
	}

	public <D> Promise<Void> push(MyRepositoryId<D> myRepositoryId,
			OTCommit<CommitId, D> commit) {
		return push(myRepositoryId, singleton(commit));
	}

	public <D> Promise<Void> push(MyRepositoryId<D> myRepositoryId,
			Collection<OTCommit<CommitId, D>> commits) {
		if (commits.isEmpty()) {
			return Promise.complete();
		}
		Map<CommitId, RawCommit> rawCommits = new HashMap<>();

		for (OTCommit<CommitId, D> commit : commits) {
			try {
				rawCommits.put(commit.getId(), decode(COMMIT_CODEC, commit.getSerializedData()));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		}

		return service.save(myRepositoryId.getRepositoryId(), rawCommits);
	}

	public <D> Promise<Void> updateHeads(MyRepositoryId<D> myRepositoryId, Set<CommitId> newHeads, Set<CommitId> oldHeads) {
		return service.saveHeads(
				myRepositoryId.getRepositoryId(),
				newHeads.stream()
						.map(commitId ->
								SignedData.sign(
										COMMIT_HEAD_CODEC,
										RawCommitHead.of(myRepositoryId.getRepositoryId(), commitId, now.currentTimeMillis()),
										myRepositoryId.getPrivKey()))
						.collect(toSet())
		);
	}

	public Promise<Set<CommitId>> getHeads(RepoID repositoryId) {
		return service.getHeads(repositoryId)
				.map(signedCommitHeads -> signedCommitHeads.stream()
						.filter(signedCommitHead ->
								signedCommitHead.verify(repositoryId.getOwner().getEcPublicKey()))
						.map(SignedData::getValue)
						.map(RawCommitHead::getCommitId)
						.collect(toSet()))
				.map(heads -> heads.isEmpty() ? singleton(CommitId.ofRoot()) : heads);
	}

	public AsyncSupplier<Set<CommitId>> pollHeads(RepoID repositoryId) {
		return service.pollHeads(repositoryId)
				.map(signedCommitHeads -> signedCommitHeads.stream()
						.filter(signedCommitHead ->
								signedCommitHead.verify(repositoryId.getOwner().getEcPublicKey()))
						.map(SignedData::getValue)
						.map(RawCommitHead::getCommitId)
						.collect(toSet()))
				.map(heads -> heads.isEmpty() ? singleton(CommitId.ofRoot()) : heads);
	}

	public <D> Promise<OTCommit<CommitId, D>> loadCommit(MyRepositoryId<D> myRepositoryId,
			Set<RepoID> originRepositoryIds, CommitId commitId) {
		if (commitId.isRoot()) return Promise.of(OTCommit.ofRoot(CommitId.ofRoot()));
		return Promises.firstSuccessful(
				() -> service.loadCommit(myRepositoryId.getRepositoryId(), commitId),
				() -> Promises.any(originRepositoryIds.stream()
						.map(originRepositoryId -> service.loadCommit(originRepositoryId, commitId)))
						.then(rawCommit -> service.save(myRepositoryId.getRepositoryId(), map(commitId, rawCommit))
								.map($ -> rawCommit)))
				.then(rawCommit -> ensureSimKey(myRepositoryId, originRepositoryIds, rawCommit.getSimKeyHash())
						.map(simKey -> {
							try {
								return getOTCommit(myRepositoryId, commitId, rawCommit, simKey);
							} catch (ParseException e) {
								throw new UncheckedException(e);
							}
						}));
	}

	@NotNull
	public <D> OTCommit<CommitId, D> getOTCommit(MyRepositoryId<D> myRepositoryId, CommitId commitId, RawCommit rawCommit, SimKey simKey) throws ParseException {
		List<byte[]> list = decode(COMMIT_DIFFS_CODEC, decryptAES(rawCommit.getEncryptedDiffs(), simKey.getAesKey()));
		if (list.size() != rawCommit.getParents().size()) {
			throw new ParseException("Number of commit diffs is not equal to number of parents");
		}

		Map<CommitId, List<D>> parents = new HashMap<>();
		Iterator<byte[]> it = list.iterator();
		for (CommitId parent : rawCommit.getParents()) {
			parents.put(parent, decode(myRepositoryId.getDiffsCodec(), it.next()));
		}

		return OTCommit.of(rawCommit.getEpoch(), commitId, parents.keySet(), parents::get, CommitId::getLevel)
				.withTimestamp(rawCommit.getTimestamp());
	}

	public <D> Promise<Optional<List<D>>> loadSnapshot(MyRepositoryId<D> myRepositoryId,
			Set<RepoID> originRepositoryIds, CommitId commitId) {
		return Promises.any(
				union(singleton(myRepositoryId.getRepositoryId()), originRepositoryIds).stream()
						.map(repositoryId -> loadSnapshot(myRepositoryId, repositoryId, commitId)
								.then(Promise::ofOptional)))
				.mapEx((snapshot, e) -> e == null ? Optional.of(snapshot) : Optional.empty());
	}

	public <D> Promise<Optional<List<D>>> loadSnapshot(MyRepositoryId<D> myRepositoryId,
			RepoID repositoryId, CommitId commitId) {
		if (commitId.isRoot()) return Promise.of(Optional.of(emptyList()));
		return service.loadSnapshot(repositoryId, commitId)
				.then(optionalRawSnapshot -> {
					if (!optionalRawSnapshot.isPresent()) {
						return Promise.of(Optional.empty());
					}

					SignedData<RawSnapshot> signedSnapshot = optionalRawSnapshot.get();
					if (!signedSnapshot.verify(repositoryId.getOwner().getEcPublicKey())) {
						return Promise.of(Optional.empty());
					}

					RawSnapshot rawSnapshot = signedSnapshot.getValue();
					return ensureSimKey(myRepositoryId, singleton(repositoryId), rawSnapshot.getSimKeyHash())
							.map(simKey -> decryptAES(rawSnapshot.encryptedDiffs, simKey.getAesKey()))
							.map(diffs -> {
								try {
									return decode(myRepositoryId.getDiffsCodec(), diffs);
								} catch (ParseException e) {
									throw new UncheckedException(e);
								}
							})
							.map(Optional::of);
				});
	}

	public <D> Promise<Void> saveSnapshot(MyRepositoryId<D> myRepositoryId,
			CommitId commitId, List<D> diffs) {
		return service.saveSnapshot(myRepositoryId.getRepositoryId(),
				SignedData.sign(
						SNAPSHOT_CODEC,
						RawSnapshot.of(
								myRepositoryId.getRepositoryId(),
								commitId,
								encryptAES(encodeAsArray(myRepositoryId.getDiffsCodec(), diffs), currentSimKey.getAesKey()),
								Hash.sha1(currentSimKey.getBytes())),
						myRepositoryId.getPrivKey()));
	}

	public void changeCurrentSimKey(@NotNull SimKey currentSimKey) {
		this.currentSimKey = currentSimKey;
		simKeys.put(Hash.sha1(currentSimKey.getBytes()), currentSimKey);
	}

	/**
	 * Synchronizes OT repository by merging local heads with some other heads from origin repository
	 *
	 * @param repository a local repository to be synchronized
	 * @param system     OT system that defines how operations should be transformed, squashed, etc
	 * @param otherHeads heads from the other repository that should be merged with local heads
	 * @param <D>        type of OT operation
	 * @return promise of <code>false</code> if two or more heads were merged into a merge commit that is empty,
	 * promise of <code>true</code> otherwise
	 */
	public static <D> Promise<Boolean> sync(OTRepository<CommitId, D> repository, OTSystem<D> system, Set<CommitId> otherHeads) {
		return repository.getHeads()
				.then(ourHeads -> excludeParents(repository, system, union(otherHeads, ourHeads))
						.then(filtered -> {
							if (filtered.size() == 1) {
								return repository.updateHeads(difference(filtered, ourHeads), emptySet())
										.map($ -> TRUE);
							}
							return merge(repository, system, filtered)
									.then(mergeCommit -> repository.push(mergeCommit)
											.then($ -> repository.updateHeads(difference(singleton(mergeCommit.getId()), ourHeads), emptySet()))
											.map($ -> !mergeCommit.getParents().values().stream()
													.flatMap(Collection::stream)
													.allMatch(system::isEmpty)));
						}));
	}

}
