import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Args;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.service.ServiceGraphModule;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.datakernel.di.module.Modules.combine;

// [START EXAMPLE]
public class ClientLauncher extends Launcher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Inject
	private RpcClient client;

	@Inject
	@Args
	private String[] args;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				new ClientModule());
	}

	@Override
	protected void run() throws Exception {
		int timeout = 1000;

		if (args.length < 2) {
			throw new RuntimeException("Command line args should be like following 1) --put key value   2) --get key");
		}

		switch (args[0]) {
			case "--put":
				client.<PutRequest, PutResponse>sendRequest(new PutRequest(args[1], args[2]), timeout)
						.whenComplete((response, e) -> {
							if (e == null) {
								System.out.println("put request was made successfully");
								System.out.println("previous value: " + response.getPreviousValue());
							} else {
								e.printStackTrace();
							}
							shutdown();
						});
				break;
			case "--get":
				client.<GetRequest, GetResponse>sendRequest(new GetRequest(args[1]), timeout)
						.whenComplete((response, e) -> {
							if (e == null) {
								System.out.println("get request was made successfully");
								System.out.println("value: " + response.getValue());
							} else {
								e.printStackTrace();
							}
							shutdown();
						});
				break;
			default:
				throw new RuntimeException("Error. You should use --put or --get option");
		}
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		ClientLauncher launcher = new ClientLauncher();
		launcher.launch(args);
	}
}
// [END EXAMPLE]
