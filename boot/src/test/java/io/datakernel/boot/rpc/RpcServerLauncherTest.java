package io.datakernel.boot.rpc;

import com.google.inject.Module;
import com.google.inject.Provides;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.util.Initializer;
import io.datakernel.util.guice.SimpleModule;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singletonList;

public class RpcServerLauncherTest {
	@Test
	public void testsInjector() {
		RpcServerLauncher launcher = new RpcServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new SimpleModule() {
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