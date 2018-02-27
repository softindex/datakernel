package io.datakernel.rpc.boot;

import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

/**
 * Lightweight holder for {@link RpcRequestHandler}s, message types and {@link SerializerBuilder}
 * to be provided for {@link RpcServerLauncher}.
 */
public final class RpcServerBusinessLogic {

	Map<Class<?>, RpcRequestHandler<?, ?>> handlers = new LinkedHashMap<>();
	List<Class<?>> messageTypes = new ArrayList<>();

	SerializerBuilder serializerBuilder;

	// region creators
	private RpcServerBusinessLogic() {
	}

	public static RpcServerBusinessLogic create() {
		return new RpcServerBusinessLogic();
	}
	// endregion

	public RpcServerBusinessLogic withMessageTypes(Class<?>... messageTypes) {
		checkNotNull(messageTypes);
		return withMessageTypes(asList(messageTypes));
	}

	public RpcServerBusinessLogic withMessageTypes(List<Class<?>> messageTypes) {
		this.messageTypes = messageTypes;
		return this;
	}

	public RpcServerBusinessLogic withSerializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return this;
	}

	@SuppressWarnings("unchecked")
	public <I, O> RpcServerBusinessLogic withHandler(Class<I> requestClass, Class<O> responseClass, RpcRequestHandler<I, O> handler) {
		handlers.put(requestClass, handler);
		return this;
	}
}
