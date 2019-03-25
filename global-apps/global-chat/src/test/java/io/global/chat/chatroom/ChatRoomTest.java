package io.global.chat.chatroom;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTNodeImpl;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.chat.chatroom.messages.Message;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.chat.Utils.CHAT_ROOM_CODEC;
import static io.global.chat.Utils.createMergedOTSystem;
import static io.global.chat.chatroom.messages.MessageOperation.insert;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.SignedData.sign;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public class ChatRoomTest {
	private static final OTSystem<ChatMultiOperation> otSystem = createMergedOTSystem();

	private CommitStorageStub commitStorage = new CommitStorageStub();

	private OTRepository<CommitId, ChatMultiOperation> repository1;
	private OTRepository<CommitId, ChatMultiOperation> repository2;

	private OTStateManager<CommitId, ChatMultiOperation> stateManager1;
	private OTStateManager<CommitId, ChatMultiOperation> stateManager2;

	private ChatRoomOTState state1 = new ChatRoomOTState();
	private ChatRoomOTState state2 = new ChatRoomOTState();

	private KeyPair keys1 = KeyPair.generate();
	private KeyPair keys2 = KeyPair.generate();

	private RepoID repoID1 = RepoID.of(keys1, "Repo One");
	private RepoID repoID2 = RepoID.of(keys2, "Repo Two");

	private int time;
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
		});
		OTDriver driver = new OTDriver(globalNode, SimKey.generate());

		repository1 = new OTRepositoryAdapter<>(
				driver,
				new MyRepositoryId<>(repoID1, keys1.getPrivKey(), CHAT_ROOM_CODEC),
				singleton(repoID2));
		repository2 = new OTRepositoryAdapter<>(
				driver,
				new MyRepositoryId<>(repoID2, keys2.getPrivKey(), CHAT_ROOM_CODEC),
				singleton(repoID1));

		stateManager1 = OTStateManager.create(eventloop, otSystem, OTNodeImpl.create(repository1, otSystem), state1);
		stateManager2 = OTStateManager.create(eventloop, otSystem, OTNodeImpl.create(repository2, otSystem), state2);

		await(stateManager1.checkout());
		await(stateManager2.checkout());
	}

	@Test
	public void testRepoSynchronizationSingleMessage() {
		ChatMultiOperation diffs1 = ChatMultiOperation.create()
				.withMessageOps(insert(new Message(time++, "User 1", "Hello")));

		stateManager1.add(diffs1);

		sync();

		assertMessages(set("Hello"));
	}

	@Test
	public void testRepoSynchronizationMultipleMessages() {
		ChatMultiOperation diffs1 = ChatMultiOperation.create()
				.withMessageOps(insert(new Message(time++, "User 1", "Hello")));

		ChatMultiOperation diffs2 = ChatMultiOperation.create()
				.withMessageOps(insert(new Message(time++, "User 1", "From user 1")));

		stateManager1.addAll(asList(diffs1, diffs2));

		ChatMultiOperation diffs3 = ChatMultiOperation.create()
				.withMessageOps(
						insert(new Message(time++, "User 2", "Hi")),
						insert(new Message(time++, "User 2", "From user 2"))
				);

		stateManager2.add(diffs3);

		sync();

		assertMessages(set("Hello", "Hi", "From user 1", "From user 2"));
	}

	private void assertMessages(Set<String> expectedMessages) {
		assertEquals(state1, state2);
		Set<String> messages = state1.getMessages()
				.stream()
				.map(Message::getContent)
				.collect(toSet());
		assertEquals(expectedMessages, messages);
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
