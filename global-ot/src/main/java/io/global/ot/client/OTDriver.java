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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.ot.OTCommit;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.TypeT;
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
import static io.datakernel.util.CollectionUtils.getLast;
import static io.datakernel.util.CollectionUtils.union;
import static io.global.common.CryptoUtils.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.singleton;
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

	public <D> OTCommit<CommitId, D> createCommit(MyRepositoryId<D> myRepositoryId,
			Map<CommitId, ? extends List<? extends D>> parentDiffs, long level) {
		long timestamp = now.currentTimeMillis();
		byte[] rawCommitBytes = getRawCommitBytes(myRepositoryId, parentDiffs, level, timestamp);
		CommitId commitId = CommitId.ofBytes(sha256(rawCommitBytes));
		return OTCommit.of(commitId, parentDiffs, level)
				.withTimestamp(timestamp)
				.withSerializedData(rawCommitBytes);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <D> byte[] getRawCommitBytes(MyRepositoryId<D> myRepositoryId, Map<CommitId, ? extends List<? extends D>> parentDiffs,
			long level, long timestamp) {
		EncryptedData encryptedDiffs = encryptAES(
				encodeAsArray(COMMIT_DIFFS_CODEC,
						parentDiffs.values()
								.stream()
								.map(value -> encodeAsArray(myRepositoryId.getDiffsCodec(), (List<D>) value))
								.collect(toList())),
				currentSimKey.getAesKey());
		return encodeAsArray(COMMIT_CODEC,
				RawCommit.of(
						parentDiffs.keySet(),
						encryptedDiffs,
						Hash.sha1(currentSimKey.getAesKey().getKey()),
						level,
						timestamp));
	}

	public Promise<Optional<SimKey>> getSharedKey(MyRepositoryId<?> myRepositoryId,
			PubKey senderPubKey, Hash simKeyHash) {
		return service.getSharedKey(myRepositoryId.getRepositoryId().getOwner(), simKeyHash)
				.thenApplyEx((signedSimKey, e) -> {
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
								.thenCompose(Promise::ofOptional)));
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
		Map<CommitId, RawCommit> rawCommits = new LinkedHashMap<>();

		for (OTCommit<CommitId, D> commit : commits) {
			try {
				rawCommits.put(commit.getId(), decode(COMMIT_CODEC, commit.getSerializedData()));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		}

		OTCommit<CommitId, D> lastCommit = getLast(commits);
		return service.save(
				myRepositoryId.getRepositoryId(),
				rawCommits,
				singleton(SignedData.sign(
						COMMIT_HEAD_CODEC,
						RawCommitHead.of(
								myRepositoryId.getRepositoryId(),
								lastCommit.getId(),
								lastCommit.getTimestamp()),
						myRepositoryId.getPrivKey()))
		);
	}

	public Promise<Set<CommitId>> getHeads(RepoID repositoryId) {
		return service.getHeads(repositoryId)
				.thenApply(signedCommitHeads -> signedCommitHeads.stream()
						.filter(signedCommitHead ->
								signedCommitHead.verify(repositoryId.getOwner().getEcPublicKey()))
						.map(SignedData::getValue)
						.map(RawCommitHead::getCommitId)
						.collect(toSet()));
	}

	public Promise<Set<CommitId>> getHeads(Set<RepoID> repositoryIds) {
		return Promises.toList(repositoryIds.stream().map(this::getHeads))
				.thenApply(commitIds -> commitIds.stream().flatMap(Collection::stream).collect(toSet()));
	}

	public <D> Promise<OTCommit<CommitId, D>> loadCommit(MyRepositoryId<D> myRepositoryId,
			Set<RepoID> originRepositoryIds, CommitId revisionId) {
		return Promises.firstSuccessful(
				() -> service.loadCommit(myRepositoryId.getRepositoryId(), revisionId),
				() -> Promises.any(originRepositoryIds.stream()
						.map(originRepositoryId -> service.loadCommit(originRepositoryId, revisionId))))
				.thenCompose(rawCommit -> ensureSimKey(myRepositoryId, originRepositoryIds, rawCommit.getSimKeyHash())
						.thenApply(simKey -> {
							try {
								return getOTCommit(myRepositoryId, revisionId, rawCommit, simKey);
							} catch (ParseException e) {
								throw new UncheckedException(e);
							}
						}));
	}

	@NotNull
	public <D> OTCommit<CommitId, D> getOTCommit(MyRepositoryId<D> myRepositoryId, CommitId revisionId, RawCommit rawCommit, SimKey simKey) throws ParseException {
		List<byte[]> list = decode(COMMIT_DIFFS_CODEC,
				decryptAES(rawCommit.getEncryptedDiffs(), simKey.getAesKey()));
		if (list.size() != rawCommit.getParents().size()) {
			throw new ParseException();
		}

		Map<CommitId, List<? extends D>> parents = new HashMap<>();
		Iterator<byte[]> it = list.iterator();
		for (CommitId parent : rawCommit.getParents()) {
			parents.put(parent, decode(myRepositoryId.getDiffsCodec(), it.next()));
		}

		return OTCommit.of(revisionId, parents, rawCommit.getLevel())
				.withTimestamp(rawCommit.getTimestamp());
	}

	public <D> Promise<Optional<List<D>>> loadSnapshot(MyRepositoryId<D> myRepositoryId,
			Set<RepoID> originRepositoryIds, CommitId revisionId) {
		return Promises.any(
				union(singleton(myRepositoryId.getRepositoryId()), originRepositoryIds).stream()
						.map(repositoryId -> loadSnapshot(myRepositoryId, repositoryId, revisionId)
								.thenCompose(Promise::ofOptional)))
				.thenApplyEx((snapshot, e) -> e == null ? Optional.of(snapshot) : Optional.empty());
	}

	public <D> Promise<Optional<List<D>>> loadSnapshot(MyRepositoryId<D> myRepositoryId,
			RepoID repositoryId, CommitId revisionId) {
		return service.loadSnapshot(repositoryId, revisionId)
				.thenCompose(optionalRawSnapshot -> {
					if (!optionalRawSnapshot.isPresent()) {
						return Promise.of(Optional.empty());
					}

					SignedData<RawSnapshot> signedSnapshot = optionalRawSnapshot.get();
					if (!signedSnapshot.verify(repositoryId.getOwner().getEcPublicKey())) {
						return Promise.of(Optional.empty());
					}

					RawSnapshot rawSnapshot = signedSnapshot.getValue();
					return ensureSimKey(myRepositoryId, singleton(repositoryId), rawSnapshot.getSimKeyHash())
							.thenApply(simKey -> decryptAES(rawSnapshot.encryptedDiffs, simKey.getAesKey()))
							.thenApply(diffs -> {
								try {
									return decode(myRepositoryId.getDiffsCodec(), diffs);
								} catch (ParseException e) {
									throw new UncheckedException(e);
								}
							})
							.thenApply(Optional::of);
				});
	}

	public <D> Promise<Void> saveSnapshot(MyRepositoryId<D> myRepositoryId,
			CommitId revisionId, List<D> diffs) {
		return service.saveSnapshot(myRepositoryId.getRepositoryId(),
				SignedData.sign(
						SNAPSHOT_CODEC,
						RawSnapshot.of(
								myRepositoryId.getRepositoryId(),
								revisionId,
								encryptAES(encodeAsArray(myRepositoryId.getDiffsCodec(), diffs), currentSimKey.getAesKey()),
								Hash.sha1(currentSimKey.getBytes())),
						myRepositoryId.getPrivKey()));
	}

	public void changeCurrentSimKey(@NotNull SimKey currentSimKey) {
		this.currentSimKey = currentSimKey;
		simKeys.put(Hash.sha1(currentSimKey.getBytes()), currentSimKey);
	}
}
