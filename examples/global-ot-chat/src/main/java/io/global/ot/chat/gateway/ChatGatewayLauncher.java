package io.global.ot.chat.gateway;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.ot.chat.common.Operation;
import io.global.ot.chat.operations.ChatOperation;

import static io.datakernel.codec.StructuredCodecs.*;
import static java.lang.Boolean.parseBoolean;

public class ChatGatewayLauncher extends GatewayLauncher {

	@Override
	protected StructuredCodec<Operation> getOperationCodec() {
		return StructuredCodecs.tuple(
				(a, b, c) -> c ? ChatOperation.delete(a, b) : ChatOperation.insert(a, b),
				op -> ((ChatOperation) op).getTimestamp(), LONG_CODEC,
				op -> ((ChatOperation) op).getContent(), STRING_CODEC,
				op -> ((ChatOperation) op).isTombstone(), BOOLEAN_CODEC);
	}

	public static void main(String[] args) throws Exception {
		new ChatGatewayLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
