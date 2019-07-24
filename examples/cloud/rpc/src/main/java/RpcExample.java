import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.rpc.client.sender.RpcStrategies.server;

//[START EXAMPLE]
public class RpcExample extends Launcher {
	private static final int SERVICE_PORT = 34765;

	@Inject
	private RpcClient client;

	@Inject
	private RpcServer server;

	@Inject
	private Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RpcServer rpcServer(Eventloop eventloop) {
		return RpcServer.create(eventloop)
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class,
						request -> Promise.of("Hello " + request))
				.withListenPort(SERVICE_PORT);
	}

	@Provides
	RpcClient rpcClient(Eventloop eventloop) {
		return RpcClient.create(eventloop)
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress(SERVICE_PORT)));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.defaultInstance();
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		CompletableFuture<Object> future = eventloop.submit(() ->
				client.sendRequest("World", 1000)
		);
		System.out.println("RPC result: " + future.get());
	}

	public static void main(String[] args) throws Exception {
		RpcExample example = new RpcExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
