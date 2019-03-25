package io.global.chat.chatroom;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.time.CurrentTimeProvider;
import io.global.chat.chatroom.messages.Message;
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

import java.util.Iterator;

import static io.datakernel.async.TestUtils.await;
import static io.global.chat.Utils.CHAT_ROOM_CODEC;
import static io.global.chat.Utils.createMergedOTSystem;
import static io.global.chat.chatroom.messages.MessageOperation.insert;
import static io.global.chat.chatroom.roomname.ChangeRoomName.changeName;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public class ChatRoomOTSystemTest {
	private final String auth1 = "author1";
	private final String auth2 = "author2";
	private OTStateManager<CommitId, ChatMultiOperation> stateManager1;
	private OTStateManager<CommitId, ChatMultiOperation> stateManager2;
	private ChatRoomOTState state1 = new ChatRoomOTState();
	private ChatRoomOTState state2 = new ChatRoomOTState();
	private CurrentTimeProvider now = CurrentTimeProvider.ofTimeSequence(0, 10);

	@Before
	public void setUp() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		KeyPair keys = KeyPair.generate();
		RepoID repoID = RepoID.of(keys, "Test");
		MyRepositoryId<ChatMultiOperation> myRepositoryId = new MyRepositoryId<>(repoID, keys.getPrivKey(), CHAT_ROOM_CODEC);
		LocalDiscoveryService discoveryService = LocalDiscoveryService.create(eventloop, new InMemoryAnnouncementStorage(),
				new InMemorySharedKeyStorage());
		OTDriver driver = new OTDriver(GlobalOTNodeImpl.create(eventloop, new RawServerId("test"), discoveryService,
				new CommitStorageStub(), rawServerId -> {throw new IllegalStateException();}), SimKey.generate());
		OTRepositoryAdapter<ChatMultiOperation> repository = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		OTCommit<CommitId, ChatMultiOperation> root = await(repository.createCommit(0, emptyMap(), 1));
		await(repository.pushAndUpdateHead(root));
		await(repository.saveSnapshot(root.getId(), emptyList()));

		OTSystem<ChatMultiOperation> otSystem = createMergedOTSystem();
		OTNode<CommitId, ChatMultiOperation, OTCommit<CommitId, ChatMultiOperation>> otNode = OTNodeImpl.create(repository, otSystem);
		this.stateManager1 = OTStateManager.create(eventloop, otSystem, otNode, state1);
		this.stateManager2 = OTStateManager.create(eventloop, otSystem, otNode, state2);
	}

	@Test
	public void test() {
		await(stateManager1.checkout(), stateManager2.checkout());

		stateManager1.add(ChatMultiOperation.create().withRoomNameOps(changeName(state1.getRoomName(), "My Room 1", timestamp())));
		stateManager2.add(ChatMultiOperation.create().withRoomNameOps(changeName(state2.getRoomName(), "My Room 2", timestamp())));

		sync();

		System.out.println(state1.getRoomName());
		System.out.println(state2.getRoomName());

		stateManager1.add(ChatMultiOperation.create().withMessageOps(
				insert(new Message(timestamp(), auth1, "Hello")),
				insert(new Message(timestamp(), auth1, "How are you?"))
		));

		stateManager2.add(ChatMultiOperation.create().withMessageOps(
				insert(new Message(timestamp(), auth2, "Test")),
				insert(new Message(timestamp(), auth2, "Message"))
		));

		sync();

		assertEquals(state1, state2);
		assertEquals("My Room 2", state1.getRoomName());
		Iterator<Message> iterator = state1.getMessages().iterator();
		asList(
				new Message(0, auth1, "Hello"),
				new Message(0, auth1, "How are you?"),
				new Message(0, auth2, "Test"),
				new Message(0, auth2, "Message")
		).forEach(message -> assertTrue(message.equalsWithoutTimestamp(iterator.next())));

		assertFalse(iterator.hasNext());
	}

	private long timestamp() {
		return now.currentTimeMillis();
	}

	private void sync() {
		await(stateManager1.sync());
		await(stateManager2.sync());
		await(stateManager1.sync());
	}
}
