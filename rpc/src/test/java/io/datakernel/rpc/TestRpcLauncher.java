package io.datakernel.rpc;

import io.datakernel.async.Stage;
import io.datakernel.rpc.boot.RpcServerBusinessLogic;
import io.datakernel.rpc.boot.RpcServerLauncher;
import org.junit.Test;

public class TestRpcLauncher {

	@Test
	public void injectionTest() {
		new RpcServerLauncher()
				.addModule(binder -> binder
						.bind(RpcServerBusinessLogic.class)
						.toInstance(RpcServerBusinessLogic.create()
								.withMessageTypes(String.class)
								.withHandler(String.class, String.class, request -> Stage.of("Hello " + request))))
				.testInjector();
	}
}
