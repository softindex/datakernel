package io.global.chat;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.ChatMultiOperation;
import io.global.chat.chatroom.messages.Message;
import io.global.chat.chatroom.messages.MessageOperation;
import io.global.chat.chatroom.messages.MessagesOTSystem;
import io.global.ot.name.NameOTSystem;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.ot.OTUtils.CHANGE_NAME_CODEC;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<Message> MESSAGE_CODEC = object(Message::new,
			"timestamp", Message::getTimestamp, LONG_CODEC,
			"author", Message::getAuthor, STRING_CODEC,
			"content", Message::getContent, STRING_CODEC);

	public static final StructuredCodec<MessageOperation> MESSAGE_OPERATION_CODEC = object(MessageOperation::new,
			"message", MessageOperation::getMessage, MESSAGE_CODEC,
			"remove", MessageOperation::isTombstone, BOOLEAN_CODEC);

	public static final StructuredCodec<ChatMultiOperation> CHAT_ROOM_CODEC = object(ChatMultiOperation::new,
			"messageOps", ChatMultiOperation::getMessageOps, ofList(MESSAGE_OPERATION_CODEC),
			"roomNameOps", ChatMultiOperation::getRoomNameOps, ofList(CHANGE_NAME_CODEC));

	public static OTSystem<ChatMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(ChatMultiOperation::new,
				ChatMultiOperation::getMessageOps, MessagesOTSystem.createOTSystem(),
				ChatMultiOperation::getRoomNameOps, NameOTSystem.createOTSystem());
	}
}
