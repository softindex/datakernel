package io.datakernel.rpc.boot;

import com.google.inject.Provides;
import io.datakernel.async.Stage;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.util.Initializer;
import io.datakernel.util.guice.SimpleModule;

public class DemoRpcBusinessLogicModule extends SimpleModule {
	@Provides
	Initializer<RpcServer> rpcServerInitializer() {
		return server -> server
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class,
						req -> Stage.of("Request: " + req));
	}
}
