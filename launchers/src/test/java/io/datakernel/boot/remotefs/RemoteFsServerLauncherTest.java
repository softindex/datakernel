package io.datakernel.boot.remotefs;

import org.junit.Test;

public class RemoteFsServerLauncherTest {
	@Test
	public void testInjector() {
		new RemoteFsServerLauncher() {}.testInjector();
	}
}