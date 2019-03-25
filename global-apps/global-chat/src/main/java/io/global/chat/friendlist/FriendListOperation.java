package io.global.chat.friendlist;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.object;
import static io.global.chat.Utils.PUB_KEY_HEX_CODEC;

public final class FriendListOperation {
	public static final FriendListOperation EMPTY = new FriendListOperation(null, true);
	public static final StructuredCodec<FriendListOperation> FRIEND_LIST_CODEC = object(FriendListOperation::new,
			"friend", FriendListOperation::getFriend, PUB_KEY_HEX_CODEC.nullable(),
			"remove", FriendListOperation::isRemove, StructuredCodecs.BOOLEAN_CODEC);

	@Nullable
	private final PubKey friend;
	private final boolean remove;

	private FriendListOperation(@Nullable PubKey friend, boolean remove) {
		this.friend = friend;
		this.remove = remove;
	}

	public static FriendListOperation add(PubKey friend) {
		return new FriendListOperation(friend, false);
	}

	public static FriendListOperation remove(PubKey friend) {
		return new FriendListOperation(friend, true);
	}

	public void apply(Set<PubKey> friends) {
		if (isEmpty()) {
			return;
		}

		if (remove) {
			friends.remove(friend);
		} else {
			friends.add(friend);
		}
	}

	public PubKey getFriend() {
		assert friend != null;
		return friend;
	}

	public boolean isRemove() {
		return remove;
	}

	public FriendListOperation invert() {
		return new FriendListOperation(friend, !remove);
	}

	public boolean isEmpty() {
		return friend == null;
	}

	public boolean isInversionFor(FriendListOperation other) {
		return friend != null && other.friend != null &&
				friend.equals(other.friend) &&
				remove != other.remove;
	}
}
