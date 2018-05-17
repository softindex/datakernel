package io.datakernel.launchers.rpc;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
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