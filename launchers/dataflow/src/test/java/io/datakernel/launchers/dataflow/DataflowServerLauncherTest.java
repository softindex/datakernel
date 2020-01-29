package io.datakernel.launchers.dataflow;

import io.datakernel.dataflow.server.DataflowEnvironment;
import io.datakernel.di.annotation.Provides;
import org.junit.Test;

public class DataflowServerLauncherTest {
	@Test
	public void testsInjector() {
		DataflowServerLauncher launcher = new DataflowServerLauncher() {
			@Provides
			public DataflowEnvironment environment() {
				throw new UnsupportedOperationException();
			}
		};
		launcher.testInjector();
	}
}
