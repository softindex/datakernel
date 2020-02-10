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

import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.common.time.SteppingCurrentTimeProvider;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StubHttpClient;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggerConfig;
import io.datakernel.test.rules.LoggingRule;
import io.global.common.*;
import io.global.common.api.EncryptedData;
import io.global.ot.api.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.common.collection.CollectionUtils.*;
import static io.datakernel.promise.TestUtils.await;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static io.global.ot.util.TestUtils.getCommitEntries;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.event.Level.WARN;

@LoggerConfig(logger = SteppingCurrentTimeProvider.class, value = WARN)
public class GlobalOTNodeHttpClientTest {
	private static final LinkedList<Object> params = new LinkedList<>();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final CurrentTimeProvider now = SteppingCurrentTimeProvider.create(10, 10);

	private final StubHttpClient stubClient = StubHttpClient.of(RoutingServlet.create().map("/ot/*", getServlet()));
	private final HttpGlobalOTNode client = HttpGlobalOTNode.create("http://localhost", stubClient);
	private final KeyPair keys = KeyPair.generate();
	private final PrivKey privKey = keys.getPrivKey();
	private final PubKey pubKey = keys.getPubKey();
	private final SimKey simKey = SimKey.generate();
	private final SharedSimKey sharedSimKey = SharedSimKey.of(simKey, pubKey);
	private final RepoID repository = RepoID.of(pubKey, "test");
	private final RawCommit rootCommit = RawCommit.of(0, emptySet(),
			EncryptedData.encrypt(new byte[0], simKey),
			Hash.sha1(simKey.getBytes()),
			0);
	private final CommitId rootCommitId = CommitId.ofCommitData(1, encode(REGISTRY.get(RawCommit.class), rootCommit).asArray());
	private final RawCommitHead rawCommitHead = RawCommitHead.of(repository, rootCommitId, 123L);
	private final SignedData<RawCommitHead> signedRawCommitHead = SignedData.sign(REGISTRY.get(RawCommitHead.class), rawCommitHead, privKey);
	private final List<CommitEntry> commitEntries = getCommitEntries(10);

	@Test
	public void list() {
		doTest(client.list(pubKey), pubKey);
	}

	@Test
	public void save() {
		Map<CommitId, RawCommit> commits = map(rootCommitId, rootCommit);
		doTest(client.save(repository, commits), repository, commits);
	}

	@Test
	public void updateHeads() {
		Set<SignedData<RawCommitHead>> newHeads = singleton(signedRawCommitHead);
		doTest(client.saveHeads(repository, newHeads), repository, newHeads);
	}

	@Test
	public void loadCommit() {
		doTest(client.loadCommit(repository, rootCommitId), repository, rootCommitId);
	}

	@Test
	public void saveSnapshot() {
		SignedData<RawSnapshot> signedSnapshot = SignedData.sign(
				REGISTRY.get(RawSnapshot.class),
				RawSnapshot.of(
						repository,
						rootCommitId,
						EncryptedData.encrypt(new byte[100], simKey),
						Hash.sha1(simKey.getBytes())),
				privKey);
		doTest(client.saveSnapshot(repository, signedSnapshot), repository, signedSnapshot);
	}

	@Test
	public void loadSnapshot() {
		doTest(client.loadSnapshot(repository, rootCommitId), repository, rootCommitId);
	}

	@Test
	public void getHeads() {
		doTest(client.getHeads(repository), repository);
	}

	@Test
	public void shareKey() {
		SignedData<SharedSimKey> signedSharedSimKey = SignedData.sign(
				REGISTRY.get(SharedSimKey.class),
				sharedSimKey,
				privKey);
		doTest(client.shareKey(pubKey, signedSharedSimKey), pubKey, signedSharedSimKey);
	}

	@Test
	public void getSharedKey() {
		Hash hash = sharedSimKey.getHash();
		doTest(client.getSharedKey(pubKey, hash), pubKey, hash);
	}

	@Test
	public void getSharedKeys() {
		doTest(client.getSharedKeys(pubKey), pubKey);
	}

	@Test
	public void sendPullRequest() {
		RawPullRequest pullRequest = RawPullRequest.of(repository, RepoID.of(pubKey, "Fork"));
		SignedData<RawPullRequest> signedPullRequest = SignedData.sign(REGISTRY.get(RawPullRequest.class), pullRequest, privKey);
		doTest(client.sendPullRequest(signedPullRequest), signedPullRequest);
	}

	@Test
	public void getPullRequests() {
		doTest(client.getPullRequests(repository), repository);
	}

	@Test
	public void upload() {
		List<CommitId> commitIds = commitEntries.stream().map(CommitEntry::getCommitId).collect(toList());
		Set<SignedData<RawCommitHead>> heads = singleton(toSignedHead(getLast(commitIds)));
		Promise<Void> uploadFinished = ChannelSupplier.ofIterable(commitEntries)
				.streamTo(ChannelConsumer.ofPromise(client.upload(repository, heads)));
		doTest(uploadFinished, repository, heads, commitIds);
	}

	@Test
	public void download() {
		CommitId head = getLast(commitEntries).getCommitId();
		List<CommitId> commitIds = new ArrayList<>();
		Set<CommitId> heads = singleton(head);
		Promise<Void> downloadFinished = ChannelSupplier.ofPromise(client.download(repository, heads))
				.streamTo(ChannelConsumer.ofConsumer(entry -> commitIds.add(entry.getCommitId())));
		doTest(downloadFinished, repository, heads, commitIds);
	}

	@Test
	public void listSnapshots() {
		doTest(client.listSnapshots(repository), repository);
	}

	// region helpers
	@NotNull
	public RawServerServlet getServlet() {
		return RawServerServlet.create(new GlobalOTNode() {
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
			public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits) {
				return resultOf(null, repositoryId, commits);
			}

			@Override
			public Promise<Void> saveHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads) {
				return resultOf(null, repositoryId, newHeads);
			}

			@Override
			public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
				return resultOf(rootCommit, repository, id);
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
			public Promise<Set<CommitId>> listSnapshots(RepoID repositoryId) {
				Set<CommitId> commitIds = set(
						CommitId.of(1, new byte[]{1, 2, 3}),
						CommitId.of(2, new byte[]{4, 5, 6}),
						CommitId.of(3, new byte[]{7, 8, 9})
				);
				return resultOf(commitIds, repositoryId);
			}

			@Override
			public Promise<Set<SignedData<RawCommitHead>>> getHeads(RepoID repositoryId) {
				return resultOf(set(signedRawCommitHead), repositoryId);
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

			@Override
			public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> startNodes) {
				return Promise.of(
						new ChannelSupplier<CommitEntry>() {
							private int index = -1;

							@NotNull
							@Override
							public Promise<CommitEntry> get() {
								index++;
								return index == commitEntries.size() ?
										resultOf(null, repositoryId, startNodes, commitEntries.stream().map(CommitEntry::getCommitId).collect(Collectors.toList())) :
										Promise.of(commitEntries.get(index));
							}

							@Override
							public void close(@NotNull Throwable e) {
							}
						}
				);
			}

			@Override
			public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId, Set<SignedData<RawCommitHead>> heads) {
				return Promise.of(new ChannelConsumer<CommitEntry>() {
					List<CommitId> commitIds = new ArrayList<>();

					@NotNull
					@Override
					public Promise<Void> accept(@Nullable CommitEntry value) {
						if (value != null) {
							commitIds.add(value.getCommitId());
							return Promise.complete();
						} else {
							return resultOf(null, repositoryId, heads, commitIds);
						}
					}

					@Override
					public void close(@NotNull Throwable e) {
					}
				});
			}
		});
	}

	private static <T> void doTest(Promise<T> promise, Object... parameters) {
		T result = await(promise);
		assertEquals(params.remove(), result);
		for (Object param : parameters) {
			assertEquals(params.remove(), param);
		}
		assertTrue(params.isEmpty());
	}

	private SignedData<RawCommitHead> toSignedHead(CommitId commitId) {
		return SignedData.sign(
				REGISTRY.get(RawCommitHead.class),
				RawCommitHead.of(repository, commitId, now.currentTimeMillis()),
				privKey);
	}

	// endregion
}
