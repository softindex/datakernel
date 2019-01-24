package io.global.ot.chat.client;


import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTStateManager;
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
	private final OTAlgorithms<CommitId, ChatOperation> algorithms;
	private final Duration pushInterval;
	private final Duration pullInterval;

	public StateManagerProvider(Eventloop eventloop, OTAlgorithms<CommitId, ChatOperation> algorithms, Duration pushInterval, Duration pullInterval) {
		this.eventloop = eventloop;
		this.algorithms = algorithms;
		this.pushInterval = pushInterval;
		this.pullInterval = pullInterval;
	}

	public Promise<OTStateManager<CommitId, ChatOperation>> get(HttpRequest request) {
		try {
			String sessionId = request.getCookie(SESSION_ID);
			OTStateManager<CommitId, ChatOperation> stateManager = entries.get(sessionId);
			if (stateManager == null) {
				stateManager = OTStateManager.create(eventloop, algorithms, new ChatOTState());
				entries.put(sessionId, stateManager);
				EventloopTaskScheduler pushScheduler = EventloopTaskScheduler.create(eventloop, stateManager::commitAndPush)
						.withInterval(pushInterval);
				EventloopTaskScheduler pullScheduler = EventloopTaskScheduler.create(eventloop, stateManager::pull).withInterval(pullInterval);
				OTStateManager<CommitId, ChatOperation> finalStateManager1 = stateManager;
				return stateManager.start()
						.thenCompose($ -> pullScheduler.start().both(pushScheduler.start()))
						.thenApply($ -> finalStateManager1);
			} else {
				return Promise.of(stateManager);
			}
		} catch (ParseException e) {
			return Promise.ofException(e);
		}

	}
}
