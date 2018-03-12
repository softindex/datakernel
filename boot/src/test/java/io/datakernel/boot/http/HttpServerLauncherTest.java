package io.datakernel.boot.http;

import com.google.inject.Module;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singletonList;

public class HttpServerLauncherTest {
	@Test
	public void testsInjector() {
		HttpServerLauncher launcher = new HttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(HelloWorldServletModule.create());
			}
		};
		launcher.testInjector();
	}
}