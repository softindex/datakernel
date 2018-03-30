package io.datakernel.launchers.http;

import com.google.inject.Module;
import com.google.inject.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.util.guice.SimpleModule;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singletonList;

public class HttpServerLauncherTest {
	@Test
	public void testsInjector() {
		HttpServerLauncher launcher = new HttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new SimpleModule() {
					@Provides
					public AsyncServlet provide() {
						throw new UnsupportedOperationException();
					}
				});
			}
		};
		launcher.testInjector();
	}
}