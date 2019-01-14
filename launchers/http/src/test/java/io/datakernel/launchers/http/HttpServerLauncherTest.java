package io.datakernel.launchers.http;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.datakernel.http.AsyncServlet;
import org.junit.Test;

import java.util.Collection;

import static io.datakernel.stream.processor.ByteBufRule.initByteBufPool;
import static java.util.Collections.singletonList;

public class HttpServerLauncherTest {
	static {
		initByteBufPool();
	}

	@Test
	public void testsInjector() {
		HttpServerLauncher launcher = new HttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new AbstractModule() {
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
