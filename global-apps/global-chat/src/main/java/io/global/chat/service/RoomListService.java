package io.global.chat.service;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNodeImpl;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.global.chat.Utils;
import io.global.chat.chatroom.ChatMultiOperation;
import io.global.chat.roomlist.Room;
import io.global.chat.roomlist.RoomListOTState;
import io.global.chat.roomlist.RoomListOperation;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.client.RepoSynchronizer;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static io.global.chat.Utils.*;
import static io.global.chat.roomlist.RoomListOTSystem.createOTSystem;
import static java.util.Collections.emptySet;

public final class RoomListService implements EventloopService {
	private static final String ROOM_LIST_REPO_NAME = "chat/index";
	private static final String ROOM_REPO_PREFIX = "chat/room/";

	private final OTDriver driver;
	private final PrivKey privKey;
	private final OTStateManager<CommitId, RoomListOperation> stateManager;
	private final RoomListOTState state;
	private final Map<String, RepoSynchronizer<ChatMultiOperation>> synchronizers = new HashMap<>();

	private RoomListService(OTStateManager<CommitId, RoomListOperation> stateManager, OTDriver driver, PrivKey privKey) {
		this.driver = driver;
		this.privKey = privKey;
		this.stateManager = stateManager;
		this.state = (RoomListOTState) stateManager.getState();
		this.state.setListener(new RoomListListener());
	}

	public static RoomListService create(Eventloop eventloop, OTDriver driver, PrivKey privKey) {
		RepoID repoID = RepoID.of(privKey, ROOM_LIST_REPO_NAME);
		MyRepositoryId<RoomListOperation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, Utils.ROOM_LIST_OPERATION_CODEC);
		OTRepositoryAdapter<RoomListOperation> repository = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		OTSystem<RoomListOperation> system = createOTSystem();
		OTNodeImpl<CommitId, RoomListOperation, OTCommit<CommitId, RoomListOperation>> node = OTNodeImpl.create(repository, system);
		RoomListOTState state = new RoomListOTState();
		OTStateManager<CommitId, RoomListOperation> stateManager = OTStateManager.create(eventloop, system, node, state)
				.withPoll(POLL_RETRY_POLICY);
		return new RoomListService(stateManager, driver, privKey);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return stateManager.getEventloop();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return stateManager.start()
				.whenResult($ -> state.getRooms()
						.forEach(room -> ensureSynchronizer(room.getId()).sync(room.getParticipants())))
				.materialize();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return stateManager.stop()
				.then($ -> Promises.all(synchronizers.values().stream().map(RepoSynchronizer::stop)))
				.materialize();
	}

	public RepoSynchronizer<ChatMultiOperation> ensureSynchronizer(String id) {
		return synchronizers.computeIfAbsent(id, $ -> {
			Eventloop eventloop = stateManager.getEventloop();
			String repoName = ROOM_REPO_PREFIX + id;
			KeyPair keys = privKey.computeKeys();
			return RepoSynchronizer.create(eventloop, driver, createMergedOTSystem(), CHAT_ROOM_CODEC, repoName, keys);
		});
	}

	class RoomListListener implements Consumer<RoomListOperation> {
		@Override
		public void accept(RoomListOperation roomListOperation) {
			if (!stateManager.isValid() || roomListOperation.isEmpty()) return;

			Room room = roomListOperation.getRoom();
			if (roomListOperation.isRemove()) {
				synchronizers.remove(room.getId()).stop();
			} else {
				ensureSynchronizer(room.getId()).sync(room.getParticipants());
			}
		}
	}
}
