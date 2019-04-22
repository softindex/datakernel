package io.global.editor.document;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.SimKey;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.datakernel.async.TestUtils.await;
import static io.global.editor.Utils.DOCUMENT_MULTI_OPERATION_CODEC;
import static io.global.editor.Utils.createMergedOTSystem;
import static io.global.editor.document.edit.InsertOperation.insert;
import static io.global.ot.name.ChangeName.changeName;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public class DocumentOTSystemTest {
	private OTStateManager<CommitId, DocumentMultiOperation> stateManager1;
	private OTStateManager<CommitId, DocumentMultiOperation> stateManager2;
	private DocumentOTState state1 = new DocumentOTState();
	private DocumentOTState state2 = new DocumentOTState();

	@Before
	public void setUp() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		KeyPair keys = KeyPair.generate();
		RepoID repoID = RepoID.of(keys, "Test");
		MyRepositoryId<DocumentMultiOperation> myRepositoryId = new MyRepositoryId<>(repoID, keys.getPrivKey(), DOCUMENT_MULTI_OPERATION_CODEC);
		LocalDiscoveryService discoveryService = LocalDiscoveryService.create(eventloop, new InMemoryAnnouncementStorage(),
				new InMemorySharedKeyStorage());
		OTDriver driver = new OTDriver(GlobalOTNodeImpl.create(eventloop, new RawServerId("test"), discoveryService,
				new CommitStorageStub(), rawServerId -> {throw new IllegalStateException();}), SimKey.generate());
		OTRepositoryAdapter<DocumentMultiOperation> repository = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		OTCommit<CommitId, DocumentMultiOperation> root = await(repository.createCommit(0, emptyMap(), 1));
		await(repository.pushAndUpdateHead(root));
		await(repository.saveSnapshot(root.getId(), emptyList()));

		OTSystem<DocumentMultiOperation> otSystem = createMergedOTSystem();
		OTNode<CommitId, DocumentMultiOperation, OTCommit<CommitId, DocumentMultiOperation>> otNode = OTNodeImpl.create(repository, otSystem);
		this.stateManager1 = OTStateManager.create(eventloop, otSystem, otNode, state1);
		this.stateManager2 = OTStateManager.create(eventloop, otSystem, otNode, state2);
	}

	@Test
	public void test() {
		await(stateManager1.checkout(), stateManager2.checkout());

		stateManager1.add(DocumentMultiOperation.create().withDocumentNameOps(changeName(state1.getDocumentName(), "My Document 1", 10)));
		stateManager2.add(DocumentMultiOperation.create().withDocumentNameOps(changeName(state2.getDocumentName(), "My Document 2", 20)));

		sync();

		System.out.println(state1.getDocumentName());
		System.out.println(state2.getDocumentName());

		stateManager1.add(DocumentMultiOperation.create().withEditOps(
				insert(0, "ABC"),
				insert(0, "123")
		));

		stateManager2.add(DocumentMultiOperation.create().withEditOps(
				insert(0, "DEF"),
				insert(0, "456")
		));

		sync();

		assertEquals(state1, state2);
		assertEquals("My Document 2", state1.getDocumentName());
		assertEquals("123ABC456DEF", state1.getContent());
	}


	private void sync() {
		await(stateManager1.sync());
		await(stateManager2.sync());
		await(stateManager1.sync());
	}
}
