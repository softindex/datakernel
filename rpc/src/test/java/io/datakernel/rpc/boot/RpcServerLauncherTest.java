package io.datakernel.rpc.boot;

import com.google.inject.Module;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singleton;

public class RpcServerLauncherTest {
	@Test
	public void testsInjector() {
		RpcServerLauncher launcher = new RpcServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singleton(DemoRpcBusinessLogicModule.create());
			}
		};
		launcher.testInjector();
	}
}