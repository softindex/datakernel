/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalsync.client;

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.ot.OTCommit;
import io.global.common.*;
import io.global.globalsync.api.*;
import io.global.globalsync.util.BinaryDataFormats;

import java.io.IOException;
import java.util.*;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.CollectionUtils.union;
import static io.global.common.CryptoUtils.*;
import static io.global.globalsync.util.BinaryDataFormats.sizeof;
import static io.global.globalsync.util.BinaryDataFormats.writeCollection;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

public final class OTDriver {
	private final RawServer server;

	private Map<SimKeyHash, SimKey> simKeys = new HashMap<>();
	private SimKey currentSimKey;

	public OTDriver(RawServer server, List<RepositoryName> originRepositoryIds, RepositoryName myRepositoryId) {
		this.server = server;
	}

	@SuppressWarnings("unchecked")
	public <D> OTCommit<CommitId, D> createCommit(MyRepositoryId<D> myRepositoryId,
			Map<CommitId, ? extends List<? extends D>> parentDiffs, long level) {
		long timestamp = getCurrentEventloop().currentTimeMillis();
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
				new SimKeyHash(sha1(currentSimKey.getAesKey().getKey())),
				level,
				System.currentTimeMillis());
		CommitId commitId = CommitId.ofBytes(sha256(rawCommitData.toBytes()));
		return OTCommit.of(commitId, parentDiffs, level)
				.withTimestamp(timestamp)
				.withSerializedData(rawCommitData);
	}

	public Stage<Optional<SimKey>> getSharedKey(MyRepositoryId<?> myRepositoryId,
			PubKey senderPubKey, SimKeyHash simKeyHash) {
		return server.getSharedKey(senderPubKey, myRepositoryId.getRepositoryId().getPubKey(), simKeyHash)
				.thenTry(maybeSignedSimKey -> {
					if (!maybeSignedSimKey.isPresent()) {
						return Optional.empty();
					}

					SignedData<SharedSimKey> signedSimKey = maybeSignedSimKey.get();
					if (!verify(signedSimKey.toBytes(), signedSimKey.getSignature(),
							myRepositoryId.getRepositoryId().getPubKey().getEcPublicKey())) {
						return Optional.empty();
					}

					SharedSimKey sharedSimKey = signedSimKey.getData();

					SimKey simKey = SimKey.ofEncryptedSimKey(sharedSimKey.getEncryptedSimKey(),
							myRepositoryId.getPrivKey());

					if (!Arrays.equals(sha1(simKey.toBytes()), simKeyHash.toBytes())) {
						return Optional.empty();
					}

					simKeys.put(simKeyHash, simKey);
					return Optional.of(simKey);
				});
	}

	public Stage<SimKey> ensureSimKey(MyRepositoryId<?> myRepositoryId,
			Set<RepositoryName> originRepositoryIds, SimKeyHash simKeyHash) {
		return simKeys.containsKey(simKeyHash) ?
				Stage.of(simKeys.get(simKeyHash)) :
				Stages.any(union(singleton(myRepositoryId.getRepositoryId()), originRepositoryIds).stream()
						.map(RepositoryName::getPubKey)
						.collect(toSet())
						.stream()
						.map(originPubKey -> getSharedKey(myRepositoryId, originPubKey, simKeyHash)
								.thenTry(Optional::get)));
	}

	public <D> Stage<Void> push(MyRepositoryId<D> myRepositoryId,
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

	public Stage<Set<CommitId>> getHeads(RepositoryName repositoryId) {
		return server.getHeads(repositoryId)
				.thenApply(signedCommitHeads -> signedCommitHeads.stream()
						.filter(signedCommitHead ->
								verify(
										signedCommitHead.toBytes(),
										signedCommitHead.getSignature(),
										repositoryId.getPubKey().getEcPublicKey()))
						.map(SignedData::getData)
						.map(RawCommitHead::getCommitId)
						.collect(toSet()));
	}

	public Stage<Set<CommitId>> getHeads(Set<RepositoryName> repositoryIds) {
		return Stages.toList(repositoryIds.stream().map(this::getHeads))
				.thenApply(commitIds -> commitIds.stream().flatMap(Collection::stream).collect(toSet()))
				.thenException(heads -> !heads.isEmpty() ? null : new IOException());
	}

	public <D> Stage<OTCommit<CommitId, D>> loadCommit(MyRepositoryId<D> myRepositoryId,
			Set<RepositoryName> originRepositoryIds, CommitId revisionId) {
		return Stages.firstSuccessful(
				() -> server.loadCommit(myRepositoryId.getRepositoryId(), revisionId),
				() -> Stages.any(originRepositoryIds.stream()
						.map(originRepositoryId -> server.loadCommit(originRepositoryId, revisionId))))
				.thenException(rawCommit ->
						Arrays.equals(revisionId.toBytes(), sha256(rawCommit.toBytes())) ?
								null : new IOException())
				.thenCompose(rawCommit -> ensureSimKey(myRepositoryId, originRepositoryIds, rawCommit.getSimKeyHash())
						.thenTry(simKey -> {
							ByteBuf buf = ByteBuf.wrapForReading(decryptAES(
									rawCommit.getEncryptedDiffs(),
									simKey.getAesKey()));
							Map<CommitId, List<? extends D>> parents = new HashMap<>();
							for (CommitId parent : rawCommit.getParents()) {
								byte[] bytes = BinaryDataFormats.readBytes(buf);
								List<? extends D> diffs = myRepositoryId.getDiffsDeserializer().apply(bytes);
								parents.put(parent, diffs);
							}

							return OTCommit.of(revisionId, parents, rawCommit.getLevel())
									.withTimestamp(rawCommit.getTimestamp());
						}));
	}

	public <D> Stage<Optional<List<D>>> loadSnapshot(MyRepositoryId<D> myRepositoryId,
			Set<RepositoryName> originRepositoryIds, CommitId revisionId) {
		return Stages.any(
				union(singleton(myRepositoryId.getRepositoryId()), originRepositoryIds).stream()
						.map(repositoryId -> loadSnapshot(myRepositoryId, repositoryId, revisionId)
								.thenTry(Optional::get)))
				.thenApplyEx((snapshot, e) -> e == null ? Optional.of(snapshot) : Optional.empty());
	}

	public <D> Stage<Optional<List<D>>> loadSnapshot(MyRepositoryId<D> myRepositoryId,
			RepositoryName repositoryId, CommitId revisionId) {
		return server.loadSnapshot(repositoryId, revisionId)
				.thenCompose(optionalRawSnapshot -> {
					if (!optionalRawSnapshot.isPresent()) {
						return Stage.of(Optional.empty());
					}

					SignedData<RawSnapshot> signedSnapshot = optionalRawSnapshot.get();
					if (!verify(signedSnapshot.toBytes(), signedSnapshot.getSignature(),
							repositoryId.getPubKey().getEcPublicKey())) {
						return Stage.of(Optional.empty());
					}

					RawSnapshot rawSnapshot = signedSnapshot.getData();
					return ensureSimKey(myRepositoryId, singleton(repositoryId), rawSnapshot.getSimKeyHash())
							.thenTry(simKey -> decryptAES(rawSnapshot.encryptedDiffs, simKey.getAesKey()))
							.thenTry(diffs -> myRepositoryId.getDiffsDeserializer().apply(diffs))
							.thenApply(Optional::of);
				});
	}

	public <D> Stage<Void> saveSnapshot(MyRepositoryId<D> myRepositoryId,
			CommitId revisionId, List<D> diffs) {
		return server.saveSnapshot(myRepositoryId.getRepositoryId(),
				SignedData.sign(
						RawSnapshot.of(
								myRepositoryId.getRepositoryId(),
								revisionId,
								encryptAES(myRepositoryId.getDiffsSerializer().apply(diffs), currentSimKey.getAesKey()),
								new SimKeyHash(sha1(currentSimKey.toBytes()))),
						myRepositoryId.getPrivKey()));
	}

}
