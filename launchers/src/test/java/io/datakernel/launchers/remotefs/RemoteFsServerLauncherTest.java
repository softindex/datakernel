package io.datakernel.launchers.remotefs;

import org.junit.Test;

public class RemoteFsServerLauncherTest {
	@Test
	public void testInjector() {
		new RemoteFsServerLauncher() {}.testInjector();
	}
}