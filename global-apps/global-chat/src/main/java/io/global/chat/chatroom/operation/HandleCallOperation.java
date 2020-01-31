package io.global.chat.chatroom.operation;

import io.global.chat.chatroom.ChatRoomOTState;
import io.global.common.PubKey;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;

import java.util.Map;
import java.util.Objects;

import static io.datakernel.common.collection.CollectionUtils.first;
import static io.global.chat.Utils.HANDLE_CALL_SUBSYSTEM;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonList;

public final class HandleCallOperation implements ChatRoomOperation {
	private final MapOperation<PubKey, Boolean> mapOperation;

	public HandleCallOperation(MapOperation<PubKey, Boolean> mapOperation) {
		this.mapOperation = mapOperation;
	}

	public static HandleCallOperation accept(PubKey user) {
		return new HandleCallOperation(MapOperation.forKey(user, SetValue.set(null, TRUE)));
	}

	public static HandleCallOperation reject(PubKey user) {
		return new HandleCallOperation(MapOperation.forKey(user, SetValue.set(null, FALSE)));
	}

	@Override
	public void apply(ChatRoomOTState state) {
		Map<PubKey, Boolean> handled = state.getHandled();
		mapOperation.getOperations()
				.forEach((user, setValue) -> {
					assert Objects.equals(handled.get(user), setValue.getPrev());
					Boolean next = setValue.getNext();
					if (next == null) {
						handled.remove(user);
					} else {
						handled.put(user, next);
					}
				});
	}

	@Override
	public boolean isEmpty() {
		return HANDLE_CALL_SUBSYSTEM.isEmpty(mapOperation);
	}

	@Override
	public HandleCallOperation invert() {
		return new HandleCallOperation(first(HANDLE_CALL_SUBSYSTEM.invert(singletonList(mapOperation))));
	}

	public MapOperation<PubKey, Boolean> getMapOperation() {
		return mapOperation;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		HandleCallOperation that = (HandleCallOperation) o;

		if (!mapOperation.equals(that.mapOperation)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return mapOperation.hashCode();
	}

	@Override
	public String toString() {
		return "HandleCallOperation{" +
				"mapOperation=" + mapOperation +
				'}';
	}
}
