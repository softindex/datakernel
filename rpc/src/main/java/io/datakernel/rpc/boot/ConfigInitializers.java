package io.datakernel.rpc.boot;

import io.datakernel.config.Config;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.util.Initializer;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.config.ConfigInitializers.ofAbstractServer;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_PACKET_SIZE;
import static io.datakernel.rpc.server.RpcServer.MAX_PACKET_SIZE;

public class ConfigInitializers {
	private ConfigInitializers() {
	}

	public static Initializer<RpcServer> ofRpcServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config.getChild("rpc.server")))
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_PACKET_SIZE),
						config.get(ofMemSize(), "rpc.streamProtocol.maxPacketSize", MAX_PACKET_SIZE),
						config.get(ofBoolean(), "rpc.streamProtocol.compression", false))
				.withFlushDelay(config.get(ofInteger(), "rpc.flushDelay", 0));
	}
}
