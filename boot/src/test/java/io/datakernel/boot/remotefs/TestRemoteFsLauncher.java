package io.datakernel.boot.remotefs;

import org.junit.Test;

public class TestRemoteFsLauncher {
	@Test
	public void injectorTest() {
		new RemoteFsServerLauncher() {}.testInjector();
	}
}
