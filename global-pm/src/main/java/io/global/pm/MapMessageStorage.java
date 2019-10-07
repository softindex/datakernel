package io.global.pm;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.first;
import static java.util.Collections.emptyMap;

public final class MapMessageStorage implements MessageStorage {
	private final Map<PubKey, Map<String, Map<Long, SignedData<RawMessage>>>> storage = new HashMap<>();

	@Override
	public Promise<Void> store(PubKey space, String mailBox, SignedData<RawMessage> message) {
		storage.computeIfAbsent(space, $ -> new HashMap<>())
				.computeIfAbsent(mailBox, $ -> new HashMap<>())
				.put(message.getValue().getId(), message);
		return Promise.complete();
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> load(PubKey space, String mailBox) {
		Map<Long, SignedData<RawMessage>> mailbox = storage.getOrDefault(space, emptyMap()).getOrDefault(mailBox, emptyMap());
		return mailbox.isEmpty() ? Promise.of(null) : Promise.of(first(mailbox.values()));
	}

	@Override
	public Promise<Void> delete(PubKey space, String mailBox, long id) {
		storage.getOrDefault(space, emptyMap()).getOrDefault(mailBox, emptyMap()).remove(id);
		return Promise.complete();
	}
}
