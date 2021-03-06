package io.datakernel.launchers.rpc;

import io.datakernel.di.annotation.Provides;
import io.datakernel.rpc.server.RpcServer;
import org.junit.Test;

public class RpcServerLauncherTest {
	@Test
	public void testsInjector() {
		RpcServerLauncher launcher = new RpcServerLauncher() {
			@Provides
			RpcServer rpcServerInitializer() {
				throw new UnsupportedOperationException();
			}
		};
		launcher.testInjector();
	}
}
