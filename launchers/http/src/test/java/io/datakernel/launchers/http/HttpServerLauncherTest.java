package io.datakernel.launchers.http;

import io.datakernel.di.module.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

public class HttpServerLauncherTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testsInjector() {
		HttpServerLauncher launcher = new HttpServerLauncher() {
			@Provides
			public AsyncServlet servlet() {
				throw new UnsupportedOperationException();
			}
		};
		launcher.testInjector();
	}
}
