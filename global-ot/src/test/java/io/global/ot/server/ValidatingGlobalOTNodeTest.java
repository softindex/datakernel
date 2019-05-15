package io.global.ot.server;

import io.datakernel.async.RetryPolicy;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.*;
import io.global.common.api.EncryptedData;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.api.*;
import io.global.ot.stub.CommitStorageStub;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.ot.server.ValidatingGlobalOTNode.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static io.global.ot.util.HttpDataFormats.COMMIT_CODEC;
import static io.global.ot.util.RawCommitChannels.*;
import static io.global.ot.util.TestUtils.getCommitId;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

public final class ValidatingGlobalOTNodeTest {
	@Rule
	public final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final KeyPair KEYS = KeyPair.generate();
	private static final SimKey SIM_KEY = SimKey.generate();
	private static final Hash HASH = Hash.sha1(SIM_KEY.getBytes());
	private static final RepoID REPO_ID = RepoID.of(KEYS, "Test repo");
	private static final RawCommit COMMIT = RawCommit.of(0, emptySet(), EncryptedData.encrypt(new byte[]{}, SIM_KEY), HASH, 0);
	private static final CommitId SOME_COMMIT_ID = getCommitId(1);
	private static final RawSnapshot SNAPSHOT = RawSnapshot.of(REPO_ID, SOME_COMMIT_ID, EncryptedData.encrypt(new byte[]{}, SIM_KEY), HASH);
	private static final EncryptedData ENCRYPTED_DATA = EncryptedData.encrypt(new byte[]{}, SIM_KEY);

	private GlobalOTNode node;
	private CommitStorageStub commitStorage = new CommitStorageStub();

	@Before
	public void setUp() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		RawServerId rawServerId = new RawServerId("test");
		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();
		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
		LocalDiscoveryService discoveryService = LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);

		GlobalOTNodeImpl otNode = GlobalOTNodeImpl.create(eventloop, rawServerId, discoveryService, commitStorage, serverId -> null)
				.withRetryPolicy(RetryPolicy.noRetry());
		node = ValidatingGlobalOTNode.create(otNode);
	}

	@Test
	public void testSave() {
		// saving commit with invalid commit id
		Throwable exception = awaitException(node.save(REPO_ID, map(SOME_COMMIT_ID, COMMIT)));
		assertSame(INVALID_COMMIT_ID, exception);

		assertFalse(await(commitStorage.loadCommit(SOME_COMMIT_ID)).isPresent());

		// saving commit with proper commitId
		CommitId properCommitId = CommitId.ofCommitData(COMMIT.getLevel(), encodeAsArray(COMMIT_CODEC, COMMIT));
		await(node.save(REPO_ID, map(properCommitId, COMMIT)));

		assertTrue(await(commitStorage.loadCommit(properCommitId)).isPresent());
	}

	@Test
	public void testSaveHeads() {
		// stub commit
		CommitId commitId = CommitId.ofRoot();
		RawCommit rawCommit = RawCommit.of(0, emptySet(), ENCRYPTED_DATA, HASH, 0);
		await(commitStorage.saveCommit(commitId, rawCommit));

		// saving head to the wrong repository
		RawCommitHead wrongRepoHead = RawCommitHead.of(RepoID.of(KeyPair.generate(), "Wrong repo"), commitId, 0);
		SignedData<RawCommitHead> sign1 = SignedData.sign(REGISTRY.get(RawCommitHead.class), wrongRepoHead, KEYS.getPrivKey());
		Throwable exception1 = awaitException(node.saveHeads(REPO_ID, set(sign1)));

		assertSame(INVALID_REPOSITORY, exception1);
		assertTrue(await(commitStorage.getHeads(REPO_ID)).isEmpty());

		// saving head signed with wrong key
		RawCommitHead properHead = RawCommitHead.of(REPO_ID, commitId, 0);
		PrivKey invalidKey = KeyPair.generate().getPrivKey();
		SignedData<RawCommitHead> sign2 = SignedData.sign(REGISTRY.get(RawCommitHead.class), properHead, invalidKey);
		Throwable exception2 = awaitException(node.saveHeads(REPO_ID, set(sign2)));

		assertSame(INVALID_SIGNATURE, exception2);
		assertTrue(await(commitStorage.getHeads(REPO_ID)).isEmpty());

		// saving proper head
		SignedData<RawCommitHead> sign3 = SignedData.sign(REGISTRY.get(RawCommitHead.class), properHead, KEYS.getPrivKey());
		Set<SignedData<RawCommitHead>> properHeads = set(sign3);
		await(node.saveHeads(REPO_ID, properHeads));

		Set<SignedData<RawCommitHead>> actualHeads = new HashSet<>(await(commitStorage.getHeads(REPO_ID)).values());
		assertEquals(properHeads, actualHeads);
	}

	@Test
	public void testLoadCommit() {
		// loading invalid commit
		CommitId invalidCommitId = getCommitId(1);
		await(commitStorage.saveCommit(invalidCommitId, COMMIT));

		Throwable exception = awaitException(node.loadCommit(REPO_ID, invalidCommitId));
		assertSame(INVALID_COMMIT_ID, exception);

		// loading proper commit
		commitStorage.clear();
		CommitId properCommitId = CommitId.ofCommitData(COMMIT.getLevel(), encodeAsArray(COMMIT_CODEC, COMMIT));
		await(commitStorage.saveCommit(properCommitId, COMMIT));

		RawCommit commit = await(node.loadCommit(REPO_ID, properCommitId));
		assertEquals(COMMIT, commit);
	}

	@Test
	public void testSaveSnapshot() {
		SignedData<RawSnapshot> properlySignedSnapshot = SignedData.sign(REGISTRY.get(RawSnapshot.class), SNAPSHOT, KEYS.getPrivKey());

		// saving snapshot to wrong repo
		RepoID wrongRepo = RepoID.of(KeyPair.generate(), "wrongRepo");
		Throwable wrongRepoException = awaitException(node.saveSnapshot(wrongRepo, properlySignedSnapshot));

		assertSame(INVALID_REPOSITORY, wrongRepoException);
		assertFalse(await(commitStorage.loadSnapshot(wrongRepo, SOME_COMMIT_ID)).isPresent());

		// saving snapshot signed with wrong key
		PrivKey wrongKey = KeyPair.generate().getPrivKey();
		SignedData<RawSnapshot> signedWithWrongKey = SignedData.sign(REGISTRY.get(RawSnapshot.class), SNAPSHOT, wrongKey);
		Throwable wrongSignatureException = awaitException(node.saveSnapshot(REPO_ID, signedWithWrongKey));

		assertSame(INVALID_SIGNATURE, wrongSignatureException);
		assertFalse(await(commitStorage.loadSnapshot(wrongRepo, SOME_COMMIT_ID)).isPresent());

		// saving proper snapshot
		await(node.saveSnapshot(REPO_ID, properlySignedSnapshot));
		assertEquals(properlySignedSnapshot, await(commitStorage.loadSnapshot(REPO_ID, SOME_COMMIT_ID)).orElseThrow(AssertionError::new));
	}

	@Test
	public void testLoadSnapshot() {
		SignedData<RawSnapshot> properlySignedSnapshot = SignedData.sign(REGISTRY.get(RawSnapshot.class), SNAPSHOT, KEYS.getPrivKey());

		// loading snapshot signed with wrong key
		PrivKey wrongKey = KeyPair.generate().getPrivKey();
		SignedData<RawSnapshot> signedWithWrongKey = SignedData.sign(REGISTRY.get(RawSnapshot.class), SNAPSHOT, wrongKey);
		commitStorage.saveSnapshot(signedWithWrongKey);
		Throwable wrongSignatureException = awaitException(node.loadSnapshot(REPO_ID, SOME_COMMIT_ID));

		assertSame(INVALID_SIGNATURE, wrongSignatureException);

		// loading proper snapshot
		commitStorage.clear();
		commitStorage.saveSnapshot(properlySignedSnapshot);
		assertEquals(properlySignedSnapshot, await(node.loadSnapshot(REPO_ID, SOME_COMMIT_ID)).orElseThrow(AssertionError::new));
	}

	@Test
	public void testGetHeads() {
		RawCommitHead head = RawCommitHead.of(REPO_ID, SOME_COMMIT_ID, 0);
		SignedData<RawCommitHead> properlySignedHead = SignedData.sign(REGISTRY.get(RawCommitHead.class), head, KEYS.getPrivKey());

		// getting head saved to the wrong repo
		RepoID wrongRepo = RepoID.of(KeyPair.generate(), "wrongRepo");
		await(commitStorage.updateHeads(wrongRepo, singleton(properlySignedHead), emptySet()));
		Throwable repoException = awaitException(node.getHeads(wrongRepo));

		assertSame(INVALID_REPOSITORY, repoException);

		// getting head signed with wrong key
		commitStorage.clear();
		PrivKey wrongKey = KeyPair.generate().getPrivKey();
		SignedData<RawCommitHead> signedWithWrongKey = SignedData.sign(REGISTRY.get(RawCommitHead.class), head, wrongKey);
		await(commitStorage.updateHeads(REPO_ID, singleton(signedWithWrongKey), emptySet()));
		Throwable signatureException = awaitException(node.getHeads(REPO_ID));

		assertSame(INVALID_SIGNATURE, signatureException);

		// getting properly signed head from proper repo
		commitStorage.clear();
		Set<SignedData<RawCommitHead>> properHeads = singleton(properlySignedHead);
		await(commitStorage.updateHeads(REPO_ID, properHeads, emptySet()));
		Set<SignedData<RawCommitHead>> loadedHeads = await(node.getHeads(REPO_ID));

		assertEquals(properHeads, loadedHeads);
	}

	@Test
	public void testDownloadImproperCommitId() {
		RawCommit firstCommit = RawCommit.of(0, emptySet(), ENCRYPTED_DATA, HASH, 0);
		commitStorage.saveCommit(SOME_COMMIT_ID, firstCommit);
		ChannelSupplier<CommitEntry> supplier = await(node.download(REPO_ID, singleton(SOME_COMMIT_ID)));
		Throwable exception = awaitException(supplier.streamTo(ChannelConsumer.ofConsumer($ -> {})));
		assertSame(COMMIT_ID_EXCEPTION, exception);
	}

	@Test
	public void testDownloadImproperParents() {
		RawCommit firstCommit = RawCommit.of(0, emptySet(), ENCRYPTED_DATA, HASH, 0);
		CommitId firstCommitId = CommitId.ofCommitData(firstCommit.getLevel(), encodeAsArray(COMMIT_CODEC, firstCommit));
		commitStorage.saveCommit(firstCommitId, firstCommit);

		RawCommit secondCommit = RawCommit.of(0, singleton(SOME_COMMIT_ID), ENCRYPTED_DATA, HASH, 0);
		CommitId secondCommitId = CommitId.ofCommitData(secondCommit.getLevel(), encodeAsArray(COMMIT_CODEC, secondCommit));
		commitStorage.saveCommit(secondCommitId, secondCommit);

		ChannelSupplier<CommitEntry> supplier = await(node.download(REPO_ID, singleton(secondCommitId)));
		Throwable exception = awaitException(supplier.streamTo(ChannelConsumer.ofConsumer($ -> {})));
		assertSame(UNEXPECTED_END_OF_STREAM, exception);
	}

	@Test
	public void testUploadImproperParents() {
		RawCommit firstCommit = RawCommit.of(0, emptySet(), ENCRYPTED_DATA, HASH, 0);
		CommitId firstCommitId = CommitId.ofCommitData(firstCommit.getLevel(), encodeAsArray(COMMIT_CODEC, firstCommit));

		RawCommit secondCommit = RawCommit.of(0, singleton(SOME_COMMIT_ID), ENCRYPTED_DATA, HASH, 0);
		CommitId secondCommitId = CommitId.ofCommitData(secondCommit.getLevel(), encodeAsArray(COMMIT_CODEC, secondCommit));

		RawCommitHead rawCommitHead = RawCommitHead.of(REPO_ID, secondCommitId, 0);
		SignedData<RawCommitHead> head = SignedData.sign(REGISTRY.get(RawCommitHead.class), rawCommitHead, KEYS.getPrivKey());

		ChannelSupplier<CommitEntry> supplier = ChannelSupplier.of(
				new CommitEntry(secondCommitId, secondCommit),
				new CommitEntry(firstCommitId, firstCommit)
		);
		ChannelConsumer<CommitEntry> consumer = await(node.upload(REPO_ID, singleton(head)));
		Throwable exception = awaitException(supplier.streamTo(consumer));
		assertSame(UNEXPECTED_COMMIT, exception);

	}
}
