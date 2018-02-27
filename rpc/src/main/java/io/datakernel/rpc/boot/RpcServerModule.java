package io.datakernel.rpc.boot;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.util.guice.SimpleModule;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_PACKET_SIZE;
import static io.datakernel.rpc.server.RpcServer.MAX_PACKET_SIZE;

public class RpcServerModule extends SimpleModule {

	// region creators
	private RpcServerModule() {
	}

	public static RpcServerModule create() {
		return new RpcServerModule();
	}
	// endregion

	@Provides
	@Singleton
	RpcServer provideRpcServer(Config config, Eventloop eventloop, RpcServerBusinessLogic logic) {
		RpcServer server = RpcServer.create(eventloop)
				.initialize(config.get(ofAbstractServerInitializer(8080), "rpc.server"))
				.withMessageTypes(logic.messageTypes)
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_PACKET_SIZE),
						config.get(ofMemSize(), "rpc.streamProtocol.maxPacketSize", MAX_PACKET_SIZE),
						config.get(ofBoolean(), "rpc.streamProtocol.compression", false))
				.withFlushDelay(config.get(ofInteger(), "rpc.flushDelay", 0));
		//noinspection unchecked, ConstantConditions
		logic.handlers.forEach((cls, handler) -> server.withHandler((Class) cls, null, handler));
		return server;
	}
}
