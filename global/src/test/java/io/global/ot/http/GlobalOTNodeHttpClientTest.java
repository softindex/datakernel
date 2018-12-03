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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.IAsyncHttpClient;
import io.global.common.*;
import io.global.ot.api.*;
import io.global.ot.api.GlobalOTNode.Heads;
import io.global.ot.api.GlobalOTNode.HeadsInfo;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GlobalOTNodeHttpClientTest {

	@Test
	public void test1() throws ExecutionException, InterruptedException {
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

		LinkedList<Object> parameters = new LinkedList<>();

		RawServerServlet servlet = RawServerServlet.create(new GlobalOTNode() {
			<T> Promise<T> resultOf(@Nullable T result, Object... args) {
				parameters.add(result);
				parameters.addAll(asList(args));
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
				return resultOf(null, signedSimKey);
			}

			@Override
			public Promise<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey receiver, Hash simKeyHash) {
				SharedSimKey sharedSimKey = SharedSimKey.of(simKey, receiver);
				return resultOf(Optional.of(SignedData.sign(REGISTRY.get(SharedSimKey.class), sharedSimKey, privKey)), receiver, simKeyHash);
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

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		CompletableFuture<Set<String>> listFuture = client.list(pubKey)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), listFuture.get());
		assertEquals(pubKey, parameters.remove());
		assertTrue(parameters.isEmpty());

		CompletableFuture<Void> saveFuture = client.save(repository, map(rootCommitId, rootCommit), singleton(signedRawCommitHead))
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), saveFuture.get());
		assertEquals(repository, parameters.remove());
		assertEquals(map(rootCommitId, rootCommit), parameters.remove());
		assertEquals(singleton(signedRawCommitHead), parameters.remove());
		assertTrue(parameters.isEmpty());

		CompletableFuture<RawCommit> loadCommitFuture = client.loadCommit(repository, rootCommitId).toCompletableFuture()
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), loadCommitFuture.get());
		assertEquals(repository, parameters.remove());
		assertEquals(rootCommitId, parameters.remove());
		assertTrue(parameters.isEmpty());

		CompletableFuture<HeadsInfo> getHeadsInfoFuture = client.getHeadsInfo(repository).toCompletableFuture()
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), getHeadsInfoFuture.get());
		assertEquals(repository, parameters.remove());
		assertTrue(parameters.isEmpty());

		SignedData<RawSnapshot> signedSnapshot = SignedData.sign(
				REGISTRY.get(RawSnapshot.class),
				RawSnapshot.of(
						repository,
						rootCommitId,
						EncryptedData.encrypt(new byte[100], simKey),
						Hash.sha1(simKey.getBytes())),
				privKey);

		CompletableFuture<Void> saveSnapshotFuture = client.saveSnapshot(
				repository,
				signedSnapshot)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), saveSnapshotFuture.get());
		assertEquals(repository, parameters.remove());
		assertEquals(signedSnapshot, parameters.remove());
		assertTrue(parameters.isEmpty());

		CompletableFuture<Optional<SignedData<RawSnapshot>>> loadSnapshotFuture = client.loadSnapshot(
				repository,
				rootCommitId)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), loadSnapshotFuture.get());
		assertEquals(repository, parameters.remove());
		assertEquals(rootCommitId, parameters.remove());
		assertTrue(parameters.isEmpty());

		CompletableFuture<Heads> getHeadsFuture = client.getHeads(
				repository,
				set(rootCommitId))
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), getHeadsFuture.get());
		assertEquals(repository, parameters.remove());
		assertEquals(set(rootCommitId), parameters.remove());
		assertTrue(parameters.isEmpty());

		SharedSimKey sharedSimKey = SharedSimKey.of(simKey, pubKey);
		SignedData<SharedSimKey> signedSharedSimKey = SignedData.sign(
				REGISTRY.get(SharedSimKey.class),
				sharedSimKey,
				privKey);

		CompletableFuture<Void> shareKeyFuture = client.shareKey(pubKey, signedSharedSimKey)
				.toCompletableFuture();

		eventloop.run();
		assertEquals(parameters.remove(), shareKeyFuture.get());
		assertEquals(signedSharedSimKey, parameters.remove());
		assertTrue(parameters.isEmpty());
	}
}
