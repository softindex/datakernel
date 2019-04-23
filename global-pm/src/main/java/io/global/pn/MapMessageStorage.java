package io.global.pn;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pn.api.MessageStorage;
import io.global.pn.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public final class MapMessageStorage implements MessageStorage {
	private final Map<PubKey, Map<Long, SignedData<RawMessage>>> storage = new HashMap<>();

	@Override
	public Promise<Void> store(PubKey space, SignedData<RawMessage> message) {
		storage.computeIfAbsent(space, $ -> new HashMap<>()).put(message.getValue().getId(), message);
		return Promise.complete();
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> load(PubKey space) {
		Map<Long, SignedData<RawMessage>> ns = storage.get(space);
		if (ns == null || ns.isEmpty()) {
			return Promise.of(null);
		}
		return Promise.of(ns.values().iterator().next());
	}

	@Override
	public Promise<Void> delete(PubKey space, long id) {
		Map<Long, SignedData<RawMessage>> ns = storage.get(space);
		if (ns != null) {
			ns.remove(id);
		}
		return Promise.complete();
	}
}
