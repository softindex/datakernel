package io.global.chat;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.ChatMultiOperation;
import io.global.chat.chatroom.Message;
import io.global.ot.name.NameOTSystem;
import io.global.ot.set.SetOperation;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.ot.OTUtils.CHANGE_NAME_CODEC;
import static io.global.ot.set.SetOTSystem.createOTSystem;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final OTSystem<SetOperation<Message>> MESSAGE_OT_SYSTEM = createOTSystem(message -> message.getContent().isEmpty());

	public static final StructuredCodec<Message> MESSAGE_CODEC = object(Message::new,
			"timestamp", Message::getTimestamp, LONG_CODEC,
			"author", Message::getAuthor, STRING_CODEC,
			"content", Message::getContent, STRING_CODEC);

	public static final StructuredCodec<SetOperation<Message>> MESSAGE_OPERATION_CODEC = object(SetOperation::of,
			"message", SetOperation::getElement, MESSAGE_CODEC,
			"remove", SetOperation::isRemove, BOOLEAN_CODEC);

	public static final StructuredCodec<ChatMultiOperation> CHAT_ROOM_CODEC = object(ChatMultiOperation::new,
			"messageOps", ChatMultiOperation::getMessageOps, ofList(MESSAGE_OPERATION_CODEC),
			"roomNameOps", ChatMultiOperation::getRoomNameOps, ofList(CHANGE_NAME_CODEC));

	public static OTSystem<ChatMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(ChatMultiOperation::new,
				ChatMultiOperation::getMessageOps, MESSAGE_OT_SYSTEM,
				ChatMultiOperation::getRoomNameOps, NameOTSystem.createOTSystem());
	}
}
