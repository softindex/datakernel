package ${groupId};

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.ExecutionException;

public class ${mainClassName} extends Launcher {

	@Inject
	private Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RpcClient rpcClient(Eventloop eventloop) {
		return RpcClient.create(eventloop)
				.withMessageTypes(String.class);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		// your RPC client logic
	}

	public static void main(String[] args) throws Exception {
		${mainClassName} rpcApp = new ${mainClassName}();
		rpcApp.launch(args);
	}
}
