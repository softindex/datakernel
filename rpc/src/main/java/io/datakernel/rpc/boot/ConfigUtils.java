package io.datakernel.rpc.boot;

import io.datakernel.config.Config;
import io.datakernel.rpc.server.RpcServer;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.config.ConfigUtils.initializeAbstractServer;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_PACKET_SIZE;
import static io.datakernel.rpc.server.RpcServer.MAX_PACKET_SIZE;

public class ConfigUtils {
	private ConfigUtils() {
	}

	public static void initializeRpcServer(RpcServer server, Config config) {
		initializeAbstractServer(server, config.getChild("rpc.server"));
		server.withStreamProtocol(
				config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_PACKET_SIZE),
				config.get(ofMemSize(), "rpc.streamProtocol.maxPacketSize", MAX_PACKET_SIZE),
				config.get(ofBoolean(), "rpc.streamProtocol.compression", false));
		server.withFlushDelay(config.get(ofInteger(), "rpc.flushDelay", 0));
	}
}
