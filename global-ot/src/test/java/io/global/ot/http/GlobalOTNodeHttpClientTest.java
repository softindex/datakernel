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

package io.global.ot.http;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.DatakernelRunner.SkipEventloopRun;
import io.global.common.*;
import io.global.common.api.EncryptedData;
import io.global.ot.api.*;
import io.global.ot.api.GlobalOTNode.CommitEntry;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.stream.Collectors;

import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.TestUtils.await;
import static io.global.ot.server.GlobalOTNodeImplTest.createCommitEntry;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
@SkipEventloopRun
public class GlobalOTNodeHttpClientTest {
	private final LinkedList<Object> params = new LinkedList<>();

	@Test
	public void test1() {
		KeyPair keys = KeyPair.generate();
		PrivKey privKey = keys.getPrivKey();
		PubKey pubKey = keys.getPubKey();

		SimKey simKey = SimKey.generate();

		RepoID repository = RepoID.of(pubKey, "test");

		RawCommit rootCommit = RawCommit.of(emptySet(),
				EncryptedData.encrypt(new byte[0], simKey),
				Hash.sha1(simKey.getBytes()),
				0, 0L);
		CommitId rootCommitId = CommitId.ofCommitData(encode(REGISTRY.get(RawCommit.class), rootCommit).asArray());

		RawCommitHead rawCommitHead = RawCommitHead.of(repository, rootCommitId, 123L);
		SignedData<RawCommitHead> signedRawCommitHead = SignedData.sign(REGISTRY.get(RawCommitHead.class), rawCommitHead, privKey);


		RawServerServlet servlet = RawServerServlet.create(new GlobalOTNode() {
			<T> Promise<T> resultOf(@Nullable T result, Object... args) {
				params.add(result);
				params.addAll(asList(args));
				return Promise.of(result);
			}

			@Override
			public Promise<Set<String>> list(PubKey pubKey) {
				return resultOf(set("a", "b", "c"), pubKey);
			}

			@Override
			public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
				return resultOf(null, repositoryId, commits, heads);
			}

			@Override
			public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
				return resultOf(rootCommit, repository, id);
			}

			@Override
			public Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
				return resultOf(new HeadsInfo(set(rootCommitId), set(rootCommitId)), repositoryId);
			}

			@Override
			public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> required, Set<CommitId> existing) {
				return Promise.of(
						new ChannelSupplier<CommitEntry>() {
							private List<CommitEntry> entries = asList(
									createCommitEntry(emptySet(), 0, false),
									createCommitEntry(set(1), 1, false),
									createCommitEntry(set(3), 2, true)
							);
							private int index = -1;

							@Override
							public Promise<CommitEntry> get() {
								index++;
								return index == 3 ?
										resultOf(null, repositoryId, required, existing, entries.stream().map(CommitEntry::getCommitId).collect(Collectors.toList())) :
										Promise.of(entries.get(index));
							}

							@Override
							public void close(Throwable e) {
							}
						}
				);
			}

			@Override
			public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId) {
				return Promise.of(new ChannelConsumer<CommitEntry>() {
					List<CommitId> commitIds = new ArrayList<>();

					@Override
					public Promise<Void> accept(@Nullable CommitEntry value) {
						if (value != null) {
							commitIds.add(value.getCommitId());
							return Promise.complete();
						} else {
							return resultOf(null, repositoryId, commitIds);
						}
					}

					@Override
					public void close(Throwable e) {
					}
				});
			}

			@Override
			public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
				return resultOf(null, repositoryId, encryptedSnapshot);
			}

			@Override
			public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
				return resultOf(Optional.of(
						SignedData.sign(
								REGISTRY.get(RawSnapshot.class),
								RawSnapshot.of(
										repositoryId,
										rootCommitId,
										EncryptedData.encrypt(new byte[100], simKey),
										Hash.sha1(simKey.getBytes())),
								privKey)),
						repositoryId, commitId);
			}

			@Override
			public Promise<Set<CommitId>> listSnapshots(RepoID repositoryId, Set<CommitId> snapshotIds) {
				Set<CommitId> commitIds = set(
						CommitId.ofBytes(new byte[]{1, 2, 3}),
						CommitId.ofBytes(new byte[]{4, 5, 6}),
						CommitId.ofBytes(new byte[]{7, 8, 9})
				);
				return resultOf(commitIds, repositoryId, snapshotIds);
			}

			@Override
			public Promise<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads) {
				return resultOf(new Heads(set(signedRawCommitHead), set(rootCommitId)),
						repositoryId, remoteHeads);
			}

			@Override
			public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> signedSimKey) {
				return resultOf(null, receiver, signedSimKey);
			}

			@Override
			public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash simKeyHash) {
				SharedSimKey sharedSimKey = SharedSimKey.of(simKey, receiver);
				return resultOf(SignedData.sign(REGISTRY.get(SharedSimKey.class), sharedSimKey, privKey), receiver, simKeyHash);
			}

			@Override
			public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
				return resultOf(emptyList(), receiver);
			}

			@Override
			public Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
				return resultOf(null,
						pullRequest);
			}

			@Override
			public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
				return resultOf(set(SignedData.sign(
						REGISTRY.get(RawPullRequest.class),
						RawPullRequest.of(repositoryId, RepoID.of(pubKey, "fork")), privKey)),
						repositoryId);
			}
		});

		GlobalOTNodeHttpClient client = GlobalOTNodeHttpClient.create(servlet::serve, "http://localhost/");

		doTest(client.list(pubKey), pubKey);

		Map<CommitId, RawCommit> commits = map(rootCommitId, rootCommit);
		Set<SignedData<RawCommitHead>> heads = singleton(signedRawCommitHead);
		doTest(client.save(repository, commits, heads), repository, commits, heads);

		doTest(client.loadCommit(repository, rootCommitId), repository, rootCommitId);

		doTest(client.getHeadsInfo(repository), repository);

		SignedData<RawSnapshot> signedSnapshot = SignedData.sign(
				REGISTRY.get(RawSnapshot.class),
				RawSnapshot.of(
						repository,
						rootCommitId,
						EncryptedData.encrypt(new byte[100], simKey),
						Hash.sha1(simKey.getBytes())),
				privKey);
		doTest(client.saveSnapshot(repository, signedSnapshot), repository, signedSnapshot);

		doTest(client.loadSnapshot(repository, rootCommitId), repository, rootCommitId);

		Set<CommitId> commitSet = set(rootCommitId);
		doTest(client.getHeads(repository, commitSet), repository, commitSet);

		SharedSimKey sharedSimKey = SharedSimKey.of(simKey, pubKey);
		SignedData<SharedSimKey> signedSharedSimKey = SignedData.sign(
				REGISTRY.get(SharedSimKey.class),
				sharedSimKey,
				privKey);
		doTest(client.shareKey(pubKey, signedSharedSimKey), pubKey, signedSharedSimKey);


		Hash hash = sharedSimKey.getHash();
		doTest(client.getSharedKey(pubKey, hash), pubKey, hash);

		doTest(client.getSharedKeys(pubKey), pubKey);

		RawPullRequest pullRequest = RawPullRequest.of(repository, RepoID.of(pubKey, "Fork"));
		SignedData<RawPullRequest> signedPullRequest = SignedData.sign(REGISTRY.get(RawPullRequest.class), pullRequest, privKey);
		doTest(client.sendPullRequest(signedPullRequest), signedPullRequest);

		doTest(client.getPullRequests(repository), repository);

		List<CommitEntry> entries = asList(
				createCommitEntry(emptySet(), 0, false),
				createCommitEntry(set(1), 1, false),
				createCommitEntry(set(3), 2, true)
		);
		List<CommitId> ids = entries.stream().map(CommitEntry::getCommitId).collect(toList());
		MaterializedPromise<Void> uploadFinished = ChannelSupplier.ofIterable(entries)
				.streamTo(ChannelConsumer.ofPromise(client.upload(repository)));
		doTest(uploadFinished, repository, ids);

		Set<CommitId> required = emptySet();
		Set<CommitId> existing = emptySet();
		ids.clear();
		MaterializedPromise<Void> downloadFinished = ChannelSupplier.ofPromise(client.download(repository, required, existing))
				.streamTo(ChannelConsumer.ofConsumer(entry -> ids.add(entry.getCommitId())));
		doTest(downloadFinished, repository, required, existing, ids);

		doTest(client.listSnapshots(repository, emptySet()), repository, emptySet());
	}

	private <T> void doTest(Promise<T> promise, Object... parameters) {
		T result = await(promise);
		assertEquals(params.remove(), result);
		for (Object param : parameters) {
			assertEquals(params.remove(), param);
		}
		assertTrue(params.isEmpty());
	}
}