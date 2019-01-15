package io.global.ot.chat.client;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.ConstantException;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;
import io.global.ot.chat.common.Gateway;
import io.global.ot.chat.operations.ChatOTState;
import io.global.ot.chat.operations.ChatOTState.ChatEntry;
import io.global.ot.chat.operations.ChatOperation;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static io.global.ot.chat.operations.ChatOperation.delete;
import static io.global.ot.chat.operations.ChatOperation.insert;
import static java.util.Collections.singletonList;

public class ChatStateManager implements EventloopService {
	public final static Duration PUSH_RETRY = ApplicationSettings.getDuration(ChatStateManager.class, "pushRetry", Duration.ofSeconds(30));
	public final static ConstantException PUSH_REJECTED = new ConstantException(ChatStateManager.class, "Push to remote node has failed");
	private final ChatOTState localState = new ChatOTState();
	private final Gateway<ChatOperation> gateway;
	private final Eventloop eventloop;
	private final List<ChatOperation> pendingOperations = new LinkedList<>();

	private boolean pushScheduled = false;
	private CommitId currentCommitId;

	private ChatStateManager(Eventloop eventloop, Gateway<ChatOperation> gateway) {
		this.eventloop = eventloop;
		this.gateway = gateway;
	}

	public static ChatStateManager create(Eventloop eventloop, Gateway<ChatOperation> gateway) {
		return new ChatStateManager(eventloop, gateway);
	}

	public Promise<Void> sendOperation(long timestamp, String content, boolean delete) {
		ChatOperation operation = delete ? delete(timestamp, content) : insert(timestamp, content);
		pendingOperations.add(operation);
		localState.apply(operation);

		return pendingOperations.size() == 1 ?
				push(operation) :
				Promise.complete();
	}

	public Promise<Set<ChatEntry>> getState() {
		return updateState()
				.thenApplyEx(($, e) -> localState.getChatEntries());
	}

	private Promise<Void> updateState() {
		return gateway.pull(currentCommitId)
				.thenCompose(this::updateState);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<Void> start() {
		localState.init();
		return gateway
				.checkout()
				.thenCompose(this::updateState);
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	private Promise<Void> push(ChatOperation operation) {
		return push(singletonList(operation));
	}

	private Promise<Void> push(List<ChatOperation> operations) {
		return gateway.push(currentCommitId, operations)
				.thenComposeEx((commitId, e) -> {
					if (e == null) {
						this.currentCommitId = commitId;
						this.pendingOperations.removeAll(operations);
						return Promise.complete();
					} else {
						if (!pushScheduled) {
							schedulePush();
						}
						return Promise.ofException(PUSH_REJECTED);
					}
				});
	}

	private Promise<Void> updateState(Tuple2<CommitId, List<ChatOperation>> tuple) {
		currentCommitId = tuple.getValue1();
		tuple.getValue2().forEach(localState::apply);
		return Promise.complete();
	}

	private void schedulePush() {
		pushScheduled = true;
		eventloop.delay(PUSH_RETRY, () -> push(pendingOperations)
				.whenComplete(($, e) -> {
					if (e != null && pendingOperations.isEmpty()) {
						pushScheduled = false;
					} else {
						schedulePush();
					}
				}));
	}
}
