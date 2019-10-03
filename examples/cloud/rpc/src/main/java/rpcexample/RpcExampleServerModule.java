package rpcexample;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.config.ConfigConverters.ofInteger;

public class RpcExampleServerModule extends AbstractModule {

	private RpcExampleServerModule() {
	}

	public static RpcExampleServerModule create() {
		return new RpcExampleServerModule();
	}

	@Provides
	RpcServer rpcServer(Eventloop eventloop, Config config) {
		return RpcServer.create(eventloop)
				.withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
				.withMessageTypes(String.class, Integer.class)
				.withHandler(String.class, Integer.class, in -> {
					System.out.println("Income message: " + in);
					return Promise.of(in.length());
				})
				.withListenPort(config.get(ofInteger(), "server.port"));
	}

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("protocol.compression", "false")
				.with("server.port", "9000")
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}
}
