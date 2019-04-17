package io.global.ot.server;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.ot.OTNodeImpl;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.stub.CommitStorageStub;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Set;

import static io.datakernel.async.Promises.reduce;
import static io.datakernel.async.Promises.repeat;
import static io.datakernel.async.TestUtils.await;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectorsEx.toAll;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.SignedData.sign;
import static io.global.ot.client.OTDriver.sync;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
public class GlobalOTNodeSynchronizationTest {
	private static final OTSystem<TestOp> otSystem = createTestOp();

	private final CommitStorage commitStorage1 = new CommitStorageStub();
	private final CommitStorage commitStorage2 = new CommitStorageStub();

	private OTStateManager<CommitId, TestOp> stateManager1;
	private OTStateManager<CommitId, TestOp> stateManager2;

	private TestOpState state1 = new TestOpState();
	private TestOpState state2 = new TestOpState();

	private KeyPair keys1 = PrivKey.fromString("1").computeKeys();
	private KeyPair keys2 = PrivKey.fromString("2").computeKeys();

	private RepoID repoID1 = RepoID.of(keys1, "Repo");
	private RepoID repoID2 = RepoID.of(keys2, "Repo");

	private GlobalOTNodeImpl master1;
	private GlobalOTNodeImpl master2;

	private OTRepositoryAdapter<TestOp> repository1;
	private OTRepositoryAdapter<TestOp> repository2;

	public GlobalOTNodeSynchronizationTest() throws ParseException {
	}

	@Before
	public void setUp() {
		Eventloop eventloop = getCurrentEventloop();

		RawServerId intermediateID = new RawServerId("Intermediate");
		RawServerId master1ID = new RawServerId("Master1");
		RawServerId master2ID = new RawServerId("Master2");

		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();

		AnnounceData announceData1 = AnnounceData.of(1, singleton(master1ID));
		AnnounceData announceData2 = AnnounceData.of(2, singleton(master2ID));

		SignedData<AnnounceData> signedData1 = sign(REGISTRY.get(AnnounceData.class), announceData1, keys1.getPrivKey());
		SignedData<AnnounceData> signedData2 = sign(REGISTRY.get(AnnounceData.class), announceData2, keys2.getPrivKey());

		announcementStorage.addAnnouncements(map(keys1.getPubKey(), signedData1, keys2.getPubKey(), signedData2));

		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
		DiscoveryService discoveryService = LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);

		master1 = GlobalOTNodeImpl.create(eventloop, master1ID, discoveryService, commitStorage1, $ -> {
			throw new IllegalStateException();
		});
		master2 = GlobalOTNodeImpl.create(eventloop, master2ID, discoveryService, commitStorage2, $ -> {
			throw new IllegalStateException();
		});
		GlobalOTNodeImpl intermediate = GlobalOTNodeImpl.create(eventloop, intermediateID, discoveryService, new CommitStorageStub(), serverId -> {
			if (serverId.equals(master1ID)) return master1;
			if (serverId.equals(master2ID)) return master2;
			throw new RuntimeException();
		}).withLatencyMargin(Duration.ZERO);
		OTDriver driver = new OTDriver(intermediate, SimKey.generate());

		MyRepositoryId<TestOp> myRepositoryId1 = new MyRepositoryId<>(repoID1, keys1.getPrivKey(), OP_CODEC);
		MyRepositoryId<TestOp> myRepositoryId2 = new MyRepositoryId<>(repoID2, keys2.getPrivKey(), OP_CODEC);

		repository1 = new OTRepositoryAdapter<>(
				driver,
				myRepositoryId1,
				singleton(repoID2));
		repository2 = new OTRepositoryAdapter<>(
				driver,
				myRepositoryId2,
				singleton(repoID1));

		stateManager1 = OTStateManager.create(eventloop, otSystem, OTNodeImpl.create(repository1, otSystem), state1).withPoll();
		stateManager2 = OTStateManager.create(eventloop, otSystem, OTNodeImpl.create(repository2, otSystem), state2).withPoll();

		await(stateManager1.start());
		await(stateManager2.start());
	}

	@Test
	public void testRepoSynchronizationSingleMessage() {
		startSyncing();

		stateManager1.add(add(10));

		syncAll();

		System.out.println(await(repository1.getHeads()));
		System.out.println(await(repository2.getHeads()));

		assertSynced(10);
	}

	@Test
	public void testRepoSynchronizationMultipleMessages() {
		startSyncing();

		stateManager1.add(add(10));
		await(stateManager1.sync());

		assertSynced(10);

		stateManager2.add(add(20));
		await(stateManager2.sync());

		assertSynced(30);

		stateManager1.add(add(30));
		stateManager2.add(add(40));
		syncAll();

		assertSynced(100);
	}

	@Test
	public void testRepoSynchronizationNotEmpty() {
		stateManager1.add(add(10));
		stateManager2.add(add(20));

		syncAll();

		startSyncing();

		assertSynced(30);

		stateManager1.add(add(100));
		await(stateManager1.sync());

		assertSynced(130);
	}

	private void assertSynced(int expectedState) {
		assertEquals(state1.getValue(), state2.getValue());
		assertEquals(expectedState, state1.getValue());

		Set<CommitId> heads1 = await(commitStorage1.getHeads(repoID1)).keySet();
		Set<CommitId> heads2 = await(commitStorage2.getHeads(repoID2)).keySet();
		assertEquals(heads1, heads2);

		assertTrue(await(reduce(toAll(), 1, heads1.stream().map(commitStorage1::isCompleteCommit).iterator())));
		assertTrue(await(reduce(toAll(), 1, heads2.stream().map(commitStorage2::isCompleteCommit).iterator())));
	}

	private void startSyncing() {
		AsyncSupplier<Set<CommitId>> heads1Supplier = repository1.pollHeads();
		AsyncSupplier<Set<CommitId>> heads2Supplier = repository2.pollHeads();

		repeat(() -> heads1Supplier.get().then(heads -> sync(repository2, otSystem, heads)));
		repeat(() -> heads2Supplier.get().then(heads -> sync(repository1, otSystem, heads)));
	}

	private void syncAll() {
		await(stateManager1.sync());
		await(stateManager2.sync());
	}
}

