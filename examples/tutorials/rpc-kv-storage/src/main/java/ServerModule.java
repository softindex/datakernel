import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

// [START EXAMPLE]
public class ServerModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError());
	}

	@Provides
	KeyValueStore keyValueStore() {
		return new KeyValueStore();
	}

	@Provides
	RpcServer rpcServer(Eventloop eventloop, KeyValueStore store) {
		return RpcServer.create(eventloop)
				.withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withHandler(PutRequest.class, PutResponse.class, req -> Promise.of(new PutResponse(store.put(req.getKey(), req.getValue()))))
				.withHandler(GetRequest.class, GetResponse.class, req -> Promise.of(new GetResponse(store.get(req.getKey()))))
				.withListenPort(RPC_SERVER_PORT);
	}
}
// [END EXAMPLE]
