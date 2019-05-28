package io.datakernel.launchers.rpc;

import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.util.Initializer;
import org.junit.Test;


import java.util.Collection;

import static java.util.Collections.singletonList;

public class RpcServerLauncherTest {
	@Test
	public void testsInjector() {
		RpcServerLauncher launcher = new RpcServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new AbstractModule() {
					@Provides
					Initializer<RpcServer> rpcServerInitializer() {
						throw new UnsupportedOperationException();
					}
				});
			}
		};
		launcher.testInjector();
	}
}
