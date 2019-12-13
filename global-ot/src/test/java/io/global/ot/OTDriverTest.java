package io.global.ot;

import io.datakernel.common.parse.ParseException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.RawServerId;
import io.global.common.SimKey;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.stub.CommitStorageStub;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public final class OTDriverTest {
	private static final OTSystem<TestOp> OT_SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private KeyPair keys1 = PrivKey.fromString("1").computeKeys();
	private KeyPair keys2 = PrivKey.fromString("2").computeKeys();

	private RepoID repoID1 = RepoID.of(keys1, "Repo");
	private RepoID repoID2 = RepoID.of(keys2, "Repo");

	private OTRepositoryAdapter<TestOp> localRepo;
	private OTRepositoryAdapter<TestOp> originRepo;

	public OTDriverTest() throws ParseException {
	}

	@Before
	public void setUp() {
		Eventloop eventloop = getCurrentEventloop();

		RawServerId serverId = new RawServerId("stub");

		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();

		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
		DiscoveryService discoveryService = LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);

		GlobalOTNode node = GlobalOTNodeImpl.create(eventloop, serverId, discoveryService, new CommitStorageStub(),
				$ -> {
					throw new IllegalStateException();
				})
				.withPollMasterRepositories(false);

		MyRepositoryId<TestOp> myRepositoryId1 = new MyRepositoryId<>(repoID1, keys1.getPrivKey(), OP_CODEC);
		MyRepositoryId<TestOp> myRepositoryId2 = new MyRepositoryId<>(repoID2, keys2.getPrivKey(), OP_CODEC);

		OTDriver driver = new OTDriver(node, SimKey.generate());

		localRepo = new OTRepositoryAdapter<>(
				driver,
				myRepositoryId1,
				singleton(repoID2));
		originRepo = new OTRepositoryAdapter<>(
				driver,
				myRepositoryId2,
				singleton(repoID1));
	}

	@Test
	public void testSyncOriginIsAhead() {
		Set<CommitId> initialHeads = await(originRepo.getHeads());
		assertEquals(1, initialHeads.size());
		CommitId initialHead = first(initialHeads);
		OTCommit<CommitId, TestOp> newCommit = await(originRepo.createCommit(initialHead, singletonList(add(10)), initialHead.getLevel()));
		await(originRepo.pushAndUpdateHead(newCommit));
		assertEquals(initialHeads, await(localRepo.getHeads()));

		Set<CommitId> originHeads = await(originRepo.getHeads());
		assertNotEquals(initialHeads, originHeads);
		assertTrue(await(OTDriver.sync(localRepo, OT_SYSTEM, singleton(newCommit.getId()))));
		assertEquals(originHeads, await(localRepo.getHeads()));
	}

	@Test
	public void testSyncOriginIsSameAsLocalSingleHead() {
		Set<CommitId> initialHeads = await(originRepo.getHeads());
		assertEquals(1, initialHeads.size());
		CommitId initialHead = first(initialHeads);
		OTCommit<CommitId, TestOp> newCommit = await(originRepo.createCommit(initialHead, singletonList(add(10)), initialHead.getLevel()));
		await(localRepo.pushAndUpdateHead(newCommit));
		await(originRepo.pushAndUpdateHead(newCommit));
		assertTrue(await(OTDriver.sync(localRepo, OT_SYSTEM, singleton(newCommit.getId()))));
	}

	@Test
	public void testSyncEmptyMerge() {
		Set<CommitId> initialHeads = await(originRepo.getHeads());
		assertEquals(1, initialHeads.size());
		CommitId initialHead = first(initialHeads);

		OTCommit<CommitId, TestOp> newCommit1 = await(originRepo.createCommit(initialHead, singletonList(add(10)), initialHead.getLevel()));
		OTCommit<CommitId, TestOp> newCommit2 = await(originRepo.createCommit(initialHead, singletonList(set(0, 100)), initialHead.getLevel()));

		await(localRepo.pushAndUpdateHead(newCommit1));
		await(originRepo.pushAndUpdateHead(newCommit2));

		Set<CommitId> originHeads = await(originRepo.getHeads());
		Set<CommitId> localHeads = await(localRepo.getHeads());

		// first merge is not empty, but creates different heads in both repos
		assertTrue(await(OTDriver.sync(localRepo, OT_SYSTEM, originHeads)));
		assertTrue(await(OTDriver.sync(originRepo, OT_SYSTEM, localHeads)));

		originHeads = await(originRepo.getHeads());
		localHeads = await(localRepo.getHeads());

		// result of this merge is empty as 2 identical heads are being merged
		assertFalse(await(OTDriver.sync(originRepo, OT_SYSTEM, localHeads)));
		assertFalse(await(OTDriver.sync(localRepo, OT_SYSTEM, originHeads)));
	}

}
