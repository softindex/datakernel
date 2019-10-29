package io.global.documents.document;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTUplinkImpl;
import io.datakernel.promise.RetryPolicy;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.SimKey;
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
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.stub.CommitStorageStub;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.promise.TestUtils.await;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.SignedData.sign;
import static io.global.documents.Utils.DOCUMENT_MULTI_OPERATION_CODEC;
import static io.global.documents.Utils.createMergedOTSystem;
import static io.global.ot.edit.InsertOperation.insert;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class DocumentTest {
	private static final OTSystem<DocumentMultiOperation> otSystem = createMergedOTSystem();

	@Rule
	public EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	private CommitStorageStub commitStorage = new CommitStorageStub();

	private OTRepository<CommitId, DocumentMultiOperation> repository1;
	private OTRepository<CommitId, DocumentMultiOperation> repository2;

	private OTStateManager<CommitId, DocumentMultiOperation> stateManager1;
	private OTStateManager<CommitId, DocumentMultiOperation> stateManager2;

	private DocumentOTState state1 = new DocumentOTState();
	private DocumentOTState state2 = new DocumentOTState();

	private KeyPair keys1 = KeyPair.generate();
	private KeyPair keys2 = KeyPair.generate();

	private RepoID repoID1 = RepoID.of(keys1, "Repo");
	private RepoID repoID2 = RepoID.of(keys2, "Repo");

	@Before
	public void setUp() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		RawServerId rawServerId = new RawServerId("Test");
		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();
		AnnounceData announceData = AnnounceData.of(1, singleton(rawServerId));
		SignedData<AnnounceData> signedData = sign(REGISTRY.get(AnnounceData.class), announceData, keys1.getPrivKey());
		announcementStorage.addAnnouncements(map(keys1.getPubKey(), signedData, keys2.getPubKey(), signedData));

		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
		DiscoveryService discoveryService = LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
		GlobalOTNodeImpl globalNode = GlobalOTNodeImpl.create(eventloop, rawServerId, discoveryService, commitStorage, $ -> {
			throw new IllegalStateException();
		}).withRetryPolicy(RetryPolicy.noRetry());
		OTDriver driver = new OTDriver(globalNode, SimKey.generate());

		repository1 = new OTRepositoryAdapter<>(
				driver,
				new MyRepositoryId<>(repoID1, keys1.getPrivKey(), DOCUMENT_MULTI_OPERATION_CODEC),
				singleton(repoID2));
		repository2 = new OTRepositoryAdapter<>(
				driver,
				new MyRepositoryId<>(repoID2, keys2.getPrivKey(), DOCUMENT_MULTI_OPERATION_CODEC),
				singleton(repoID1));

		stateManager1 = OTStateManager.create(eventloop, otSystem, OTUplinkImpl.create(repository1, otSystem), state1);
		stateManager2 = OTStateManager.create(eventloop, otSystem, OTUplinkImpl.create(repository2, otSystem), state2);

		await(stateManager1.checkout());
		await(stateManager2.checkout());
	}

	@Test
	public void testRepoSynchronizationSingleMessage() {
		DocumentMultiOperation diffs1 = DocumentMultiOperation.create()
				.withEditOps(insert(0, "Hello"));

		stateManager1.add(diffs1);

		sync();

		assertContent("Hello");
	}

	@Test
	public void testRepoSynchronizationMultipleMessages() {
		DocumentMultiOperation diffs1 = DocumentMultiOperation.create()
				.withEditOps(insert(0, "abcd"));

		DocumentMultiOperation diffs2 = DocumentMultiOperation.create()
				.withEditOps(insert(2, "ef"));

		stateManager1.addAll(asList(diffs1, diffs2));

		DocumentMultiOperation diffs3 = DocumentMultiOperation.create()
				.withEditOps(
						insert(0, "123"),
						insert(2, "45")
				);

		stateManager2.add(diffs3);

		sync();

		assertContent("12453abefcd");
	}

	private void assertContent(String expectedContent) {
		assertEquals(state1, state2);
		assertEquals(expectedContent, state1.getContent());
	}

	private void sync() {
		await(stateManager1.sync());
		await(stateManager2.sync());

		OTDriver.sync(repository1, otSystem, await(repository2.getHeads()));
		OTDriver.sync(repository2, otSystem, await(repository1.getHeads()));

		await(stateManager1.sync());
		await(stateManager2.sync());
	}
}
