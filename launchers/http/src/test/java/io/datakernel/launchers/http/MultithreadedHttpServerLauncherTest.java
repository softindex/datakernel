package io.datakernel.launchers.http;

import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import org.junit.ClassRule;
import org.junit.Test;

public class MultithreadedHttpServerLauncherTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testsInjector() {
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Provides
			@Worker
			AsyncServlet servlet(@WorkerId int worker) {
				throw new UnsupportedOperationException();
			}
		};
		launcher.testInjector();
	}
}
