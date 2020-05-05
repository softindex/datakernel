package io.datakernel.dataflow.di;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.binary.ByteBufsCodec;
import io.datakernel.dataflow.command.DataflowCommand;
import io.datakernel.dataflow.command.DataflowResponse;
import io.datakernel.dataflow.di.CodecsModule.Subtypes;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJsonBuf;
import static io.datakernel.csp.binary.ByteBufsDecoder.ofNullTerminatedBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DataflowModule extends AbstractModule {
	private DataflowModule() {
	}

	public static Module create() {
		return new DataflowModule();
	}

	@Override
	protected void configure() {
		install(DataflowCodecs.create());
		install(EnvironmentModule.create());
		install(BinarySerializersModule.create());
	}

	@Provides
	ByteBufsCodec<DataflowCommand, DataflowResponse> commandToResponse(@Subtypes StructuredCodec<DataflowCommand> command, StructuredCodec<DataflowResponse> response) {
		return nullTerminated(command, response);
	}

	@Provides
	ByteBufsCodec<DataflowResponse, DataflowCommand> responseToCommand(@Subtypes StructuredCodec<DataflowCommand> command, StructuredCodec<DataflowResponse> response) {
		return nullTerminated(response, command);
	}

	private static <I, O> ByteBufsCodec<I, O> nullTerminated(StructuredCodec<I> inputCodec, StructuredCodec<O> outputCodec) {
		return ByteBufsCodec.ofDelimiter(ofNullTerminatedBytes(), buf -> {
			ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
			buf1.put((byte) 0);
			return buf1;
		}).andThen(buf -> fromJson(inputCodec, buf.asString(UTF_8)), item -> toJsonBuf(outputCodec, item));
	}
}
