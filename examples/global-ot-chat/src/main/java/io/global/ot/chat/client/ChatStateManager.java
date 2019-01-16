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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static io.global.ot.chat.operations.ChatOperation.delete;
import static io.global.ot.chat.operations.ChatOperation.insert;
import static java.util.Collections.singletonList;

public final class ChatStateManager implements EventloopService {
	private final static Logger logger = LoggerFactory.getLogger(ChatStateManager.class);

	public final static ConstantException PUSH_REJECTED = new ConstantException(ChatStateManager.class, "Push to remote node has failed");
	public final static ConstantException COMMIT_ID_NOT_SET = new ConstantException(ChatStateManager.class, "Commit ID has not been set yet");
	public final static Duration PUSH_RETRY = ApplicationSettings.getDuration(ChatStateManager.class, "pushRetry", Duration.ofSeconds(30));
	private final ChatOTState localState = new ChatOTState();
	private final Gateway<ChatOperation> gateway;
	private final Eventloop eventloop;
	private final List<ChatOperation> pendingOperations = new LinkedList<>();

	private boolean pushScheduled = false;

	@Nullable
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

		if (pendingOperations.size() == 1) {
			push(operation);
		}

		return Promise.complete();
	}

	public Promise<Set<ChatEntry>> getState() {
		return updateState()
				.thenApplyEx(($, e) -> localState.getChatEntries());
	}

	@SuppressWarnings("ConstantConditions")
	private Promise<Void> updateState() {
		return ensureCurrentCommitId()
				.thenCompose($ -> gateway.pull(currentCommitId))
				.thenCompose(this::updateState);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<Void> start() {
		localState.init();
		return tryCheckout();
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	private Promise<Void> push(ChatOperation operation) {
		return push(singletonList(operation));
	}

	@SuppressWarnings("ConstantConditions")
	private Promise<Void> push(List<ChatOperation> operations) {
		return ensureCurrentCommitId()
				.thenCompose($ -> gateway.push(currentCommitId, operations))
				.thenComposeEx((commitId, e) -> {
					if (e == null) {
						this.currentCommitId = commitId;
						this.pendingOperations.removeAll(operations);
						return Promise.complete();
					} else {
						schedulePush();
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
		if (pushScheduled) {
			return;
		}
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

	private Promise<Void> ensureCurrentCommitId() {
		return currentCommitId != null ?
				Promise.complete() :
				tryCheckout()
						.thenCompose($ -> {
							if (currentCommitId == null) {
								return Promise.ofException(COMMIT_ID_NOT_SET);
							}
							return Promise.complete();
						});
	}

	private Promise<Void> tryCheckout() {
		return gateway.checkout()
				.thenComposeEx((tuple, e) -> {
					if (e == null) {
						return updateState(tuple);
					} else {
						logger.trace("Could not checkout from gateway", e);
						return Promise.complete();
					}
				});
	}
}
