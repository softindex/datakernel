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
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.DatakernelRunner.SkipEventloopRun;
import io.global.common.*;
import io.global.common.api.EncryptedData;
import io.global.ot.api.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.TestUtils.await;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
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
				throw new UnsupportedOperationException();
			}

			@Override
			public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId) {
				throw new UnsupportedOperationException();
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

		IAsyncHttpClient httpClient = request -> {
			try {
				return servlet.serve(request);
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};

		GlobalOTNodeHttpClient client = GlobalOTNodeHttpClient.create(httpClient, "http://localhost/");

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
