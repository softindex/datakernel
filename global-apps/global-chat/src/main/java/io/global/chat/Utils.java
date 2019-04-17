package io.global.chat;

import io.datakernel.async.AsyncFunction1;
import io.datakernel.async.Promise;
import io.datakernel.async.RetryPolicy;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.ChatMultiOperation;
import io.global.chat.chatroom.messages.Message;
import io.global.chat.chatroom.messages.MessageOperation;
import io.global.chat.chatroom.messages.MessagesOTSystem;
import io.global.chat.chatroom.roomname.ChangeRoomName;
import io.global.chat.chatroom.roomname.RoomNameOTSystem;
import io.global.chat.roomlist.Room;
import io.global.chat.roomlist.RoomListOperation;
import io.global.common.PrivKey;
import io.global.common.PubKey;

import static io.datakernel.codec.StructuredCodecs.*;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final AsyncFunction1<HttpRequest, PrivKey> DEMO_PRIV_KEY_FUNCTION = request -> {
		try {
			return Promise.of(PrivKey.fromString(request.getCookie("Key")));
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	};

	public static final RetryPolicy POLL_RETRY_POLICY = RetryPolicy.exponentialBackoff(1000, 1000 * 60);

	public static final StructuredCodec<PubKey> PUB_KEY_HEX_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);

	public static final StructuredCodec<Room> ROOM_CODEC = object(Room::new,
			"id", Room::getId, STRING_CODEC,
			"participants", Room::getParticipants, ofSet(PUB_KEY_HEX_CODEC));

	public static final StructuredCodec<RoomListOperation> ROOM_LIST_OPERATION_CODEC = object(RoomListOperation::new,
			"room", RoomListOperation::getRoom, ROOM_CODEC,
			"remove", RoomListOperation::isRemove, BOOLEAN_CODEC);

	public static final StructuredCodec<Message> MESSAGE_CODEC = object(Message::new,
			"timestamp", Message::getTimestamp, LONG_CODEC,
			"author", Message::getAuthor, STRING_CODEC,
			"content", Message::getContent, STRING_CODEC);

	public static final StructuredCodec<MessageOperation> MESSAGE_OPERATION_CODEC = object(MessageOperation::new,
			"message", MessageOperation::getMessage, MESSAGE_CODEC,
			"remove", MessageOperation::isTombstone, BOOLEAN_CODEC);

	public static final StructuredCodec<ChangeRoomName> CHANGE_ROOM_NAME_CODEC = object(ChangeRoomName::new,
			"prev", ChangeRoomName::getPrev, STRING_CODEC,
			"next", ChangeRoomName::getNext, STRING_CODEC,
			"timestamp", ChangeRoomName::getTimestamp, LONG_CODEC);

	public static final StructuredCodec<ChatMultiOperation> CHAT_ROOM_CODEC = object(ChatMultiOperation::new,
			"messageOps", ChatMultiOperation::getMessageOps, ofList(MESSAGE_OPERATION_CODEC),
			"roomNameOps", ChatMultiOperation::getRoomNameOps, ofList(Utils.CHANGE_ROOM_NAME_CODEC));

	public static OTSystem<ChatMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(ChatMultiOperation::new,
				ChatMultiOperation::getMessageOps, MessagesOTSystem.createOTSystem(),
				ChatMultiOperation::getRoomNameOps, RoomNameOTSystem.createOTSystem());
	}
}
