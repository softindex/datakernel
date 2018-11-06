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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.ot.OTCommit;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.*;
import io.global.ot.api.*;
import io.global.ot.util.BinaryDataFormats;
import org.spongycastle.crypto.CryptoException;

import java.io.IOException;
import java.util.*;

import static io.datakernel.util.CollectionUtils.union;
import static io.global.common.CryptoUtils.*;
import static io.global.ot.util.BinaryDataFormats.sizeof;
import static io.global.ot.util.BinaryDataFormats.writeCollection;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

public final class OTDriver {
	private final RawServer server;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private Map<Hash, SimKey> simKeys = new HashMap<>();
	private SimKey currentSimKey;

	public OTDriver(RawServer server, List<RepoID> originRepositoryIds, RepoID myRepositoryId) {
		this.server = server;
	}

	@SuppressWarnings("unchecked")
	public <D> OTCommit<CommitId, D> createCommit(MyRepositoryId<D> myRepositoryId,
			Map<CommitId, ? extends List<? extends D>> parentDiffs, long level) {
		long timestamp = now.currentTimeMillis();
		List<CommitId> parents = new ArrayList<>();
		List<byte[]> diffsBytes = new ArrayList<>();
		parentDiffs.forEach((key, value) -> {
			parents.add(key);
			diffsBytes.add(myRepositoryId.getDiffsSerializer().apply((List<D>) value));
		});
		ByteBuf dataBuf = ByteBuf.wrapForWriting(new byte[sizeof(diffsBytes, BinaryDataFormats::sizeof)]);
		writeCollection(dataBuf, diffsBytes, BinaryDataFormats::writeBytes);
		EncryptedData encryptedDiffs = encryptAES(
				dataBuf.asArray(),
				currentSimKey.getAesKey());
		RawCommit rawCommitData = RawCommit.of(
				parents,
				encryptedDiffs,
				Hash.ofBytes(sha1(currentSimKey.getAesKey().getKey())),
				level,
				System.currentTimeMillis());
		CommitId commitId = CommitId.ofBytes(sha256(rawCommitData.toBytes()));
		return OTCommit.of(commitId, parentDiffs, level)
				.withTimestamp(timestamp)
				.withSerializedData(rawCommitData);
	}

	public Promise<Optional<SimKey>> getSharedKey(MyRepositoryId<?> myRepositoryId,
			PubKey senderPubKey, Hash simKeyHash) {
		return server.getSharedKey(senderPubKey, myRepositoryId.getRepositoryId().getOwner(), simKeyHash)
				.thenApply(maybeSignedSimKey -> {
					if (!maybeSignedSimKey.isPresent()) {
						return Optional.empty();
					}

					SignedData<SharedSimKey> signedSimKey = maybeSignedSimKey.get();

					if (!signedSimKey.verify(myRepositoryId.getRepositoryId().getOwner())) {
						return Optional.empty();
					}

					SimKey simKey;
					try {
						simKey = signedSimKey.getData().decryptSimKey(myRepositoryId.getPrivKey());
					} catch (CryptoException ignored) {
						return Optional.empty();
					}

					if (!Arrays.equals(sha1(simKey.toBytes()), simKeyHash.toBytes())) {
						return Optional.empty();
					}

					simKeys.put(simKeyHash, simKey);
					return Optional.of(simKey);
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
		return server.save(
				myRepositoryId.getRepositoryId(),
				(RawCommit) commit.getSerializedData(),
				SignedData.sign(
						RawCommitHead.of(
								myRepositoryId.getRepositoryId(),
								commit.getId(),
								commit.getTimestamp()),
						myRepositoryId.getPrivKey())
		);
	}

	public Promise<Set<CommitId>> getHeads(RepoID repositoryId) {
		return server.getHeads(repositoryId)
				.thenApply(signedCommitHeads -> signedCommitHeads.stream()
						.filter(signedCommitHead ->
								verify(
										signedCommitHead.toBytes(),
										signedCommitHead.getSignature(),
										repositoryId.getOwner().getEcPublicKey()))
						.map(SignedData::getData)
						.map(RawCommitHead::getCommitId)
						.collect(toSet()));
	}

	public Promise<Set<CommitId>> getHeads(Set<RepoID> repositoryIds) {
		return Promises.toList(repositoryIds.stream().map(this::getHeads))
				.thenApply(commitIds -> commitIds.stream().flatMap(Collection::stream).collect(toSet()))
				.thenException(heads -> !heads.isEmpty() ? null : new IOException());
	}

	public <D> Promise<OTCommit<CommitId, D>> loadCommit(MyRepositoryId<D> myRepositoryId,
			Set<RepoID> originRepositoryIds, CommitId revisionId) {
		return Promises.firstSuccessful(
				() -> server.loadCommit(myRepositoryId.getRepositoryId(), revisionId),
				() -> Promises.any(originRepositoryIds.stream()
						.map(originRepositoryId -> server.loadCommit(originRepositoryId, revisionId))))
				.thenException(rawCommit ->
						Arrays.equals(revisionId.toBytes(), sha256(rawCommit.toBytes())) ?
								null : new IOException())
				.thenCompose(rawCommit -> ensureSimKey(myRepositoryId, originRepositoryIds, rawCommit.getSimKeyHash())
						.thenApply(simKey -> {
							try {
								ByteBuf buf = ByteBuf.wrapForReading(decryptAES(
										rawCommit.getEncryptedDiffs(),
										simKey.getAesKey()));
								Map<CommitId, List<? extends D>> parents = new HashMap<>();
								for (CommitId parent : rawCommit.getParents()) {
									byte[] bytes = BinaryDataFormats.readBytes(buf);
									List<? extends D> diffs = myRepositoryId.getDiffsDeserializer().parse(bytes);
									parents.put(parent, diffs);
								}

								return OTCommit.of(revisionId, parents, rawCommit.getLevel())
										.withTimestamp(rawCommit.getTimestamp());
							} catch (ParseException e) {
								throw new UncheckedException(e);
							}
						}));
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
		return server.loadSnapshot(repositoryId, revisionId)
				.thenCompose(optionalRawSnapshot -> {
					if (!optionalRawSnapshot.isPresent()) {
						return Promise.of(Optional.empty());
					}

					SignedData<RawSnapshot> signedSnapshot = optionalRawSnapshot.get();
					if (!verify(signedSnapshot.toBytes(), signedSnapshot.getSignature(),
							repositoryId.getOwner().getEcPublicKey())) {
						return Promise.of(Optional.empty());
					}

					RawSnapshot rawSnapshot = signedSnapshot.getData();
					return ensureSimKey(myRepositoryId, singleton(repositoryId), rawSnapshot.getSimKeyHash())
							.thenApply(simKey -> decryptAES(rawSnapshot.encryptedDiffs, simKey.getAesKey()))
							.thenApply(diffs -> {
								try {
									return myRepositoryId.getDiffsDeserializer().parse(diffs);
								} catch (ParseException e) {
									throw new UncheckedException(e);
								}
							})
							.thenApply(Optional::of);
				});
	}

	public <D> Promise<Void> saveSnapshot(MyRepositoryId<D> myRepositoryId,
			CommitId revisionId, List<D> diffs) {
		return server.saveSnapshot(myRepositoryId.getRepositoryId(),
				SignedData.sign(
						RawSnapshot.of(
								myRepositoryId.getRepositoryId(),
								revisionId,
								encryptAES(myRepositoryId.getDiffsSerializer().apply(diffs), currentSimKey.getAesKey()),
								Hash.ofBytes(sha1(currentSimKey.toBytes()))),
						myRepositoryId.getPrivKey()));
	}

}
