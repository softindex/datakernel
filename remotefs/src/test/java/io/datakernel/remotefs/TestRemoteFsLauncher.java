package io.datakernel.remotefs;

import io.datakernel.remotefs.boot.RemoteFsServerLauncher;
import org.junit.Test;

public class TestRemoteFsLauncher {
	@Test
	public void injectorTest() {
		new RemoteFsServerLauncher() {}.testInjector();
	}
}
