import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.datakernel.rpc.client.sender.RpcStrategies.server;

public class RpcExample extends Launcher {
	private static final int SERVICE_PORT = 34765;
	static {
		LoggerConfigurer.enableLogging();
	}

	@Inject
	private RpcClient client;

	@Inject
	private RpcServer server;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RpcServer rpcServer(Eventloop eventloop) {
		return RpcServer.create(eventloop)
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, request -> Promise.of("Hello " + request))
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
	protected void run() {
		client.sendRequest("World", 1000).whenComplete((res, e) -> {
			if (e != null) {
				System.err.println("Got exception: " + e);
			} else {
				System.out.println("Got result: " + res);
			}
		});
	}

	public static void main(String[] args) throws Exception {
		RpcExample example = new RpcExample();
		example.launch(args);
	}
}
