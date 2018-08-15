package io.global.globalsync.http;

import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.global.common.*;
import io.global.globalsync.api.*;
import io.global.globalsync.api.RawServer.HeadsDelta;
import io.global.globalsync.api.RawServer.HeadsInfo;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RawServerHttpClientTest {

	@Test
	public void test1() throws ExecutionException, InterruptedException {
		PrivKey privKey = PrivKey.random();
		PubKey pubKey = PubKey.ofPrivKey(privKey);
		RepositoryName repository = new RepositoryName(pubKey, "test");

		SimKey simKey = SimKey.random();
		RawCommit rootCommit = RawCommit.of(asList(),
				EncryptedData.encrypt(new byte[0], simKey),
				SimKeyHash.ofSimKey(simKey),
				0, 0L);
		CommitId rootCommitId = CommitId.ofCommit(rootCommit);

		RawCommitHead rawCommitHead = RawCommitHead.of(repository, rootCommitId, 123L);
		SignedData<RawCommitHead> signedRawCommitHead = SignedData.sign(rawCommitHead, privKey);

		LinkedList<Object> parameters = new LinkedList<>();
		RawServerServlet servlet = RawServerServlet.create(new RawServer() {
			<T> Stage<T> resultOf(T result, Object... args) {
				parameters.add(result);
				parameters.addAll(asList(args));
				return Stage.of(result);
			}

			@Override
			public Stage<Set<String>> list(PubKey pubKey) {
				return resultOf(set("a", "b", "c"), pubKey);
			}

			@Override
			public Stage<Void> save(RepositoryName repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
				return resultOf(null, repositoryId, commits, heads);
			}

			@Override
			public Stage<RawCommit> loadCommit(RepositoryName repositoryId, CommitId id) {
				return resultOf(rootCommit, repository, id);
			}

			@Override
			public Stage<HeadsInfo> getHeadsInfo(RepositoryName repositoryId) {
				return resultOf(new HeadsInfo(set(rootCommitId), set(rootCommitId)), repositoryId);
			}

			@Override
			public Stage<StreamProducer<CommitEntry>> download(RepositoryName repositoryId, Set<CommitId> bases, Set<CommitId> heads) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Stage<StreamConsumer<CommitEntry>> upload(RepositoryName repositoryId) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Stage<Void> saveSnapshot(RepositoryName repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
				return resultOf(null, repositoryId, encryptedSnapshot);
			}

			@Override
			public Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepositoryName repositoryId, CommitId commitId) {
				return resultOf(Optional.of(
						SignedData.sign(
								RawSnapshot.of(repositoryId, rootCommitId,
										EncryptedData.encrypt(new byte[100], simKey), SimKeyHash.ofSimKey(simKey)),
								privKey)),
						repositoryId, commitId);
			}

			@Override
			public Stage<HeadsDelta> getHeads(RepositoryName repositoryId, Set<CommitId> remoteHeads) {
				return resultOf(new HeadsDelta(set(signedRawCommitHead), set(rootCommitId)),
						repositoryId, remoteHeads);
			}

			@Override
			public Stage<Void> shareKey(SignedData<SharedSimKey> simKey) {
				return resultOf(null, simKey);
			}

			@Override
			public Stage<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey repositoryOwner, PubKey receiver, SimKeyHash simKeyHash) {
				return resultOf(Optional.of(
						SignedData.sign(
								SharedSimKey.of(repositoryOwner, receiver, EncryptedSimKey.ofSimKey(simKey, pubKey), simKeyHash),
								privKey)),
						repositoryOwner, receiver, simKeyHash);
			}

			@Override
			public Stage<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
				return resultOf(null,
						pullRequest);
			}

			@Override
			public Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepositoryName repositoryId) {
				return resultOf(set(SignedData.sign(
						RawPullRequest.of(repositoryId, new RepositoryName(pubKey, "fork")), privKey)),
						repositoryId);
			}
		});

		IAsyncHttpClient httpClient = new IAsyncHttpClient() {
			@Override
			public Stage<HttpResponse> send(HttpRequest request) {
				return servlet.serve(request);
			}
		};

		RawServerHttpClient client = new RawServerHttpClient(httpClient, "http://localhost/");

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
				RawSnapshot.of(repository, rootCommitId,
						EncryptedData.encrypt(new byte[100], simKey), SimKeyHash.ofSimKey(simKey)),
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

		CompletableFuture<HeadsDelta> getHeadsFuture = client.getHeads(
				repository,
				set(rootCommitId))
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), getHeadsFuture.get());
		assertEquals(repository, parameters.remove());
		assertEquals(set(rootCommitId), parameters.remove());
		assertTrue(parameters.isEmpty());

		SignedData<SharedSimKey> signedSharedSimKey = SignedData.sign(
				SharedSimKey.of(
						pubKey,
						pubKey,
						EncryptedSimKey.ofSimKey(simKey, pubKey),
						SimKeyHash.ofSimKey(simKey)),
				privKey);
		CompletableFuture<Void> shareKeyFuture = client.shareKey(signedSharedSimKey)
				.toCompletableFuture();
		eventloop.run();
		assertEquals(parameters.remove(), shareKeyFuture.get());
		assertEquals(signedSharedSimKey, parameters.remove());
		assertTrue(parameters.isEmpty());
	}
}
