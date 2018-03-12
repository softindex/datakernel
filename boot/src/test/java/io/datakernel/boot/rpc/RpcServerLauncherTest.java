package io.datakernel.boot.rpc;

import com.google.inject.Module;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singletonList;

public class RpcServerLauncherTest {
	@Test
	public void testsInjector() {
		RpcServerLauncher launcher = new RpcServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new DemoRpcBusinessLogicModule());
			}
		};
		launcher.testInjector();
	}
}