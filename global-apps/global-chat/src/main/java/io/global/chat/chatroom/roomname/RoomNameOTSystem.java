package io.global.chat.chatroom.roomname;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import static io.datakernel.ot.TransformResult.*;
import static io.datakernel.util.Preconditions.checkState;
import static io.global.chat.chatroom.roomname.ChangeRoomName.changeName;
import static java.util.Collections.singletonList;

public final class RoomNameOTSystem {
	private RoomNameOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<ChangeRoomName> createOTSystem() {
		return OTSystemImpl.<ChangeRoomName>create()
				.withEmptyPredicate(ChangeRoomName.class, ChangeRoomName::isEmpty)
				.withInvertFunction(ChangeRoomName.class, op -> singletonList(changeName(op.getNext(), op.getPrev(), op.getTimestamp())))

				.withSquashFunction(ChangeRoomName.class, ChangeRoomName.class, (first, second) -> changeName(first.getPrev(), second.getNext(), second.getTimestamp()))
				.withTransformFunction(ChangeRoomName.class, ChangeRoomName.class, (left, right) -> {
					checkState(left.getPrev().equals(right.getPrev()), "Previous values of left and right operation should be equal");
					if (left.getTimestamp() > right.getTimestamp())
						return right(changeName(right.getNext(), left.getNext(), left.getTimestamp()));
					if (left.getTimestamp() < right.getTimestamp())
						return left(changeName(left.getNext(), right.getNext(), right.getTimestamp()));
					if (left.getNext().compareTo(right.getNext()) > 0)
						return right(changeName(right.getNext(), left.getNext(), left.getTimestamp()));
					if (left.getNext().compareTo(right.getNext()) < 0)
						return left(changeName(left.getNext(), right.getNext(), right.getTimestamp()));
					return empty();
				});
	}
}
