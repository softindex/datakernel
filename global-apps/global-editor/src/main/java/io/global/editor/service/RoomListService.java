package io.global.editor.service;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNodeImpl;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.editor.Utils;
import io.global.editor.document.DocumentMultiOperation;
import io.global.editor.documentlist.Document;
import io.global.editor.documentlist.DocumentListOTState;
import io.global.editor.documentlist.DocumentListOperation;
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

import static io.global.editor.Utils.*;
import static io.global.editor.documentlist.DocumentListOTSystem.createOTSystem;
import static java.util.Collections.emptySet;

public final class RoomListService implements EventloopService {
	private static final String ROOM_LIST_REPO_NAME = "chat/index";
	private static final String ROOM_REPO_PREFIX = "chat/room/";

	private final OTDriver driver;
	private final PrivKey privKey;
	private final OTStateManager<CommitId, DocumentListOperation> stateManager;
	private final DocumentListOTState state;
	private final Map<String, RepoSynchronizer<DocumentMultiOperation>> synchronizers = new HashMap<>();

	private RoomListService(OTStateManager<CommitId, DocumentListOperation> stateManager, OTDriver driver, PrivKey privKey) {
		this.driver = driver;
		this.privKey = privKey;
		this.stateManager = stateManager;
		this.state = (DocumentListOTState) stateManager.getState();
		this.state.setListener(new RoomListListener());
	}

	public static RoomListService create(Eventloop eventloop, OTDriver driver, PrivKey privKey) {
		RepoID repoID = RepoID.of(privKey, ROOM_LIST_REPO_NAME);
		MyRepositoryId<DocumentListOperation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, Utils.DOCUMENT_LIST_OPERATION_CODEC);
		OTRepositoryAdapter<DocumentListOperation> repository = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		OTSystem<DocumentListOperation> system = createOTSystem();
		OTNodeImpl<CommitId, DocumentListOperation, OTCommit<CommitId, DocumentListOperation>> node = OTNodeImpl.create(repository, system);
		DocumentListOTState state = new DocumentListOTState();
		OTStateManager<CommitId, DocumentListOperation> stateManager = OTStateManager.create(eventloop, system, node, state)
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
				.whenResult($ -> state.getDocuments()
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

	public RepoSynchronizer<DocumentMultiOperation> ensureSynchronizer(String id) {
		return synchronizers.computeIfAbsent(id, $ -> {
			Eventloop eventloop = stateManager.getEventloop();
			String repoName = ROOM_REPO_PREFIX + id;
			KeyPair keys = privKey.computeKeys();
			return RepoSynchronizer.create(eventloop, driver, createMergedOTSystem(), DOCUMENT_MULTI_OPERATION_CODEC, repoName, keys);
		});
	}

	class RoomListListener implements Consumer<DocumentListOperation> {
		@Override
		public void accept(DocumentListOperation documentListOperation) {
			if (!stateManager.isValid() || documentListOperation.isEmpty()) return;

			Document document = documentListOperation.getDocument();
			if (documentListOperation.isRemove()) {
				synchronizers.remove(document.getId()).stop();
			} else {
				ensureSynchronizer(document.getId()).sync(document.getParticipants());
			}
		}
	}
}
