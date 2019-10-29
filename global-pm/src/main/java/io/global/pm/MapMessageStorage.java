package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelSupplier;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.AsyncExecutors.ofMaxRecursiveCalls;
import static java.util.Collections.emptyMap;
import static java.util.Comparator.comparingLong;

public final class MapMessageStorage implements MessageStorage {
	private static final int MAX_RECURSIVE_CALLS = 10;

	final Map<PubKey, Map<String, Map<Long, SignedData<RawMessage>>>> storage = new HashMap<>();

	@Override
	public Promise<Boolean> put(PubKey space, String mailBox, SignedData<RawMessage> message) {
		RawMessage newValue = message.getValue();
		SignedData<RawMessage> computed = getMailBox(space, mailBox)
				.compute(newValue.getId(), (id, old) -> {
					if (old == null) {
						return message;
					}
					RawMessage oldValue = old.getValue();
					if (oldValue.isTombstone() || oldValue.getTimestamp() > newValue.getTimestamp()) {
						return old;
					}
					return message;
				});
		return Promise.of(computed == message);
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox) {
		return Promise.of(getMailBox(space, mailBox)
				.values()
				.stream()
				.filter(signedData -> signedData.getValue().isMessage())
				.sorted(comparingLong(signedData -> signedData.getValue().getTimestamp()))
				.findAny()
				.orElse(null));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp) {
		return Promise.of(ChannelSupplier.ofStream(getMailBox(space, mailBox).values()
				.stream()
				.filter(signedData -> signedData.getValue().getTimestamp() > timestamp)
				.sorted(comparingLong(signed -> signed.getValue().getTimestamp())))
				.withExecutor(ofMaxRecursiveCalls(MAX_RECURSIVE_CALLS)));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return Promise.of(storage.getOrDefault(space, emptyMap()).keySet());
	}

	@Override
	public Promise<Void> cleanup(long timestamp) {
		storage.forEach(($, mailBoxes) ->
				mailBoxes.forEach(($2, mailBox) ->
						mailBox.values().removeIf(signedData -> {
							RawMessage rawMessage = signedData.getValue();
							return rawMessage.isTombstone() && rawMessage.getTimestamp() < timestamp;
						})));
		return Promise.complete();
	}

	private Map<Long, SignedData<RawMessage>> getMailBox(PubKey space, String mailBox) {
		return storage.computeIfAbsent(space, $ -> new HashMap<>())
				.computeIfAbsent(mailBox, $ -> new HashMap<>());
	}
}
