package advancedrpc;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promises;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.stream.IntStream.range;

public class AdvancedRpcClientApp extends Launcher {
	@Inject
	RpcClient client;

	@Inject
	Eventloop eventloop;

	@Override
	protected Module getModule() {
		return ModuleBuilder.create()
				.install(ServiceGraphModule.create())
				.install(AdvancedRpcClientModule.create())
				.build();
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		CompletableFuture<Void> future = eventloop.submit(() ->
				Promises.all(range(0, 100).mapToObj(i ->
						client.sendRequest(i, 1000)
								.whenResult(res -> System.out.println("Answer : " + res)))));
		future.get();
	}

	public static void main(String[] args) throws Exception {
		AdvancedRpcClientApp app = new AdvancedRpcClientApp();
		app.launch(args);
	}
}
