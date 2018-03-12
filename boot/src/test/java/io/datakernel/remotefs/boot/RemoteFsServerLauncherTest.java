package io.datakernel.remotefs.boot;

import org.junit.Test;

public class RemoteFsServerLauncherTest {
	@Test
	public void testInjector() {
		new RemoteFsServerLauncher() {}.testInjector();
	}
}