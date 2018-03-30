package io.datakernel.launchers.http;

import com.google.inject.Module;
import com.google.inject.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singletonList;

public class MultithreadedHttpServerLauncherTest {
	@Test
	public void testsInjector() {
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new SimpleModule() {
					@Provides
					@Worker
					AsyncServlet provideServlet(@WorkerId int worker) {
						throw new UnsupportedOperationException();
					}
				});
			}
		};
		launcher.testInjector();
	}
}