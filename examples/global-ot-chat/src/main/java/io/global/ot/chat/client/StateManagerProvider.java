package io.global.ot.chat.client;


import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.ot.OTNode;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.global.ot.api.CommitId;
import io.global.ot.chat.operations.ChatOTState;
import io.global.ot.chat.operations.ChatOperation;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.global.ot.chat.operations.Utils.SESSION_ID;

final class StateManagerProvider {
	private static final Map<String, OTStateManager<CommitId, ChatOperation>> entries = new HashMap<>();

	private final Eventloop eventloop;
	private final OTSystem<ChatOperation> otSystem;
	private final OTNode<CommitId, ChatOperation> node;
	private final Duration syncInterval;

	public StateManagerProvider(Eventloop eventloop, OTSystem<ChatOperation> otSystem, OTNode<CommitId, ChatOperation> node, Duration syncInterval) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.node = node;
		this.syncInterval = syncInterval;
	}

	public Promise<OTStateManager<CommitId, ChatOperation>> get(HttpRequest request) {
		try {
			String sessionId = request.getCookie(SESSION_ID);
			OTStateManager<CommitId, ChatOperation> stateManager = entries.get(sessionId);
			if (stateManager == null) {
				stateManager = new OTStateManager<>(otSystem, node, new ChatOTState());
				entries.put(sessionId, stateManager);
				EventloopTaskScheduler syncScheduler = EventloopTaskScheduler.create(eventloop, stateManager::sync)
						.withInterval(syncInterval);
				OTStateManager<CommitId, ChatOperation> finalStateManager1 = stateManager;
				return stateManager.checkout()
						.thenCompose($ -> syncScheduler.start())
						.thenApply($ -> finalStateManager1);
			} else {
				return Promise.of(stateManager);
			}
		} catch (ParseException e) {
			return Promise.ofException(e);
		}

	}
}
