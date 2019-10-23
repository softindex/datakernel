package io.global.chat;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.CallInfo;
import io.global.chat.chatroom.ChatRoomOTSystem;
import io.global.chat.chatroom.message.Message;
import io.global.chat.chatroom.operation.*;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;

import java.math.BigInteger;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.Utils.PUB_KEY_HEX_CODEC;
import static io.global.chat.chatroom.message.MessageType.*;
import static io.global.ot.OTUtils.getMapOperationCodec;

public final class Utils {
	public static final PubKey STUB_PUB_KEY = PrivKey.of(BigInteger.ONE).computePubKey();
	public static final CallInfo EMPTY_CALL_INFO = new CallInfo(STUB_PUB_KEY, "", -1);

	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<CallInfo> CALL_INFO_CODEC = object(CallInfo::new,
			"pubKey", CallInfo::getPubKey, PUB_KEY_HEX_CODEC,
			"peerId", CallInfo::getPeerId, STRING_CODEC,
			"timestamp", CallInfo::getCallStarted, LONG_CODEC);

	public static final StructuredCodec<CallOperation> CALL_OPERATION_CODEC = object(CallOperation::new,
			"prev", CallOperation::getPrev, CALL_INFO_CODEC.nullable(),
			"next", CallOperation::getNext, CALL_INFO_CODEC.nullable());

	public static final StructuredCodec<DropCallOperation> DROP_CALL_OPERATION_CODEC = object(DropCallOperation::new,
			"callInfo", DropCallOperation::getCallInfo, CALL_INFO_CODEC,
			"handled", DropCallOperation::getPreviouslyHandled, ofMap(PUB_KEY_HEX_CODEC, BOOLEAN_CODEC),
			"dropTimestamp", DropCallOperation::getDropTimestamp, LONG_CODEC,
			"invert", DropCallOperation::isInvert, BOOLEAN_CODEC);

	public static final StructuredCodec<HandleCallOperation> HANDLE_CALL_OPERATION_CODEC =
			getMapOperationCodec(PUB_KEY_HEX_CODEC, BOOLEAN_CODEC)
					.transform(HandleCallOperation::new, HandleCallOperation::getMapOperation);

	public static final StructuredCodec<MessageOperation> MESSAGE_OPERATION_CODEC = object(MessageOperation::new,
			"timestamp", MessageOperation::getTimestamp, LONG_CODEC,
			"author", MessageOperation::getAuthor, PUB_KEY_HEX_CODEC,
			"content", MessageOperation::getContent, STRING_CODEC,
			"invert", MessageOperation::isInvert, BOOLEAN_CODEC);

	public static final StructuredCodec<ChatRoomOperation> CHAT_ROOM_OPERATION_CODEC = CodecSubtype.<ChatRoomOperation>create()
			.with(CallOperation.class, "Call", CALL_OPERATION_CODEC)
			.with(DropCallOperation.class, "Drop", DROP_CALL_OPERATION_CODEC)
			.with(HandleCallOperation.class, "Handle", HANDLE_CALL_OPERATION_CODEC)
			.with(MessageOperation.class, "Message", MESSAGE_OPERATION_CODEC)
			.withTagName("type", "value");

	public static final OTSystem<MapOperation<PubKey, Boolean>> HANDLE_CALL_SUBSYSTEM = MapOTSystem
			.create(Boolean::compareTo);

	public static final OTSystem<ChatRoomOperation> CHAT_ROOM_OT_SYSTEM = ChatRoomOTSystem.createOTSystem();

	public static Message toRegularMessage(MessageOperation operation) {
		return new Message(REGULAR, operation.getTimestamp(), operation.getAuthor(), operation.getContent());
	}

	public static Message toCallMessage(CallInfo callInfo) {
		return new Message(CALL, callInfo.getCallStarted(), callInfo.getPubKey(), "Call info: " + callInfo);
	}

	public static Message toDropMessage(DropCallOperation dropCallOperation) {
		CallInfo callInfo = dropCallOperation.getCallInfo();
		String content = "Call dropped: " + callInfo +
				"\nHandled: " + dropCallOperation.getPreviouslyHandled() +
				"\nDropped at: " + dropCallOperation.getDropTimestamp();
		return new Message(DROP, dropCallOperation.getDropTimestamp(), callInfo.getPubKey(), content);
	}

	// https://www.ecma-international.org/ecma-262/6.0/#sec-number-objects
	public static final Long JS_MAX_SAFE_INTEGER = (long) Math.pow(2, 53) - 1;
	public static final Long JS_MIN_SAFE_INTEGER = -JS_MAX_SAFE_INTEGER;

}
