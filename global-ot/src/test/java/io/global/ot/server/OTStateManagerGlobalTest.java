package io.global.ot.server;

import io.datakernel.async.RetryPolicy;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.SimKey;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.DiscoveryServiceDriver;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RawCommitHead;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.stub.CommitStorageStub;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.ot.util.TestUtils.TEST_OP_CODEC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OTStateManagerGlobalTest {
	private static final OTSystem<TestOp> OT_SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private TestOpState state;
	private OTRepository<CommitId, TestOp> repository;
	private OTStateManager<CommitId, TestOp> stateManager;
	private RepoID repoID;
	private CommitStorage commitStorage;

	@Before
	public void setUp() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		KeyPair keys = KeyPair.generate();
		RawServerId rawServerId = new RawServerId("Test");
		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();
		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
		DiscoveryService discoveryService = LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
		await(DiscoveryServiceDriver.create(discoveryService)
				.announce(keys, AnnounceData.of(System.currentTimeMillis(), singleton(rawServerId))));
		commitStorage = new CommitStorageStub();
		GlobalOTNode globalNode = GlobalOTNodeImpl.create(eventloop, rawServerId, discoveryService,
				commitStorage, id -> {throw new IllegalStateException();})
				.withRetryPolicy(RetryPolicy.noRetry());
		OTDriver driver = new OTDriver(globalNode, SimKey.generate());
		repoID = RepoID.of(keys.getPubKey(), "Test");
		MyRepositoryId<TestOp> myRepositoryId = new MyRepositoryId<>(repoID, keys.getPrivKey(), TEST_OP_CODEC);
		state = new TestOpState();
		repository = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		OTNodeImpl<CommitId, TestOp, OTCommit<CommitId, TestOp>> node = OTNodeImpl.create(repository, OT_SYSTEM);
		stateManager = OTStateManager.create(eventloop, OT_SYSTEM, node, state);
	}

	@Test
	public void testCheckoutEmpty() {
		await(stateManager.checkout());

		assertTrue(stateManager.getCommitId().isRoot());
		assertEquals(0, state.getValue());

		Map<CommitId, SignedData<RawCommitHead>> realHeads = await(commitStorage.getHeads(repoID));
		assertTrue(realHeads.isEmpty());
	}

	@Test
	public void testCheckout() {
		OTCommit<CommitId, TestOp> newCommit = await(repository.createCommit(CommitId.ofRoot(), asList(add(12), set(12, 23), add(-2)), 2));
		await(repository.pushAndUpdateHead(newCommit));

		await(stateManager.checkout());
		assertEquals(21, state.getValue());

		Map<CommitId, SignedData<RawCommitHead>> realHeads = await(commitStorage.getHeads(repoID));
		assertEquals(singleton(newCommit.getId()), realHeads.keySet());
	}

	@Test
	public void testSync() {
		await(stateManager.checkout());
		List<TestOp> diffs = asList(add(12), set(12, 23), add(-2));
		await(stateManager.addAll(diffs));

		await(stateManager.sync());
		assertEquals(21, state.getValue());

		Set<CommitId> heads = await(repository.getHeads());
		assertEquals(1, heads.size());
		OTCommit<CommitId, TestOp> commit = await(repository.loadCommit(first(heads)));

		assertEquals(map(CommitId.ofRoot(), OT_SYSTEM.squash(diffs)), commit.getParents());
	}
}
