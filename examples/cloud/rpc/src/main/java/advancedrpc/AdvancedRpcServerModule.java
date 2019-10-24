package advancedrpc;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;

public class AdvancedRpcServerModule extends AbstractModule {
	private AdvancedRpcServerModule() {
	}

	public static AdvancedRpcServerModule create() {
		return new AdvancedRpcServerModule();
	}

	@Provides
	@Worker
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	Integer port(@WorkerId int workerId) {
		return 9000 + workerId;
	}

	@Provides
	@Worker
	RpcServer rpcServer(Eventloop eventloop, Integer port) {
		return RpcServer.create(eventloop)
				.withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
				.withMessageTypes(Integer.class)
				.withHandler(Integer.class, Integer.class, in -> {
					System.out.println("Income message: on port #" + port + " : " + in);
					return Promise.of(in);
				})
				.withListenPort(port);
	}
}
