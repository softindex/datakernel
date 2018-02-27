package io.datakernel.rpc.boot;

import com.google.inject.Provides;
import io.datakernel.async.Stage;
import io.datakernel.util.guice.SimpleModule;

public class DemoRpcBusinessLogicModule extends SimpleModule {

	// region creators
	private DemoRpcBusinessLogicModule() {
	}

	public static DemoRpcBusinessLogicModule create() {
		return new DemoRpcBusinessLogicModule();
	}
	// endregion

	@Provides
	protected RpcServerBusinessLogic provide() {
		return RpcServerBusinessLogic.create()
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, req ->
						Stage.of("Hello from demo DataKernel RPC server. Your request was: " + req));
	}
}
