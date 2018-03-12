package io.datakernel.boot.http;

import com.google.inject.Module;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singletonList;

public class MultithreadedHttpServerLauncherTest {
	@Test
	public void testsInjector() {
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(HelloWorldWorkerServletModule.create());
			}
		};
		launcher.testInjector();
	}
}