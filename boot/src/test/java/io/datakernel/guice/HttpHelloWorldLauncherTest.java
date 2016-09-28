package io.datakernel.guice;

import org.junit.Test;

public class HttpHelloWorldLauncherTest {
	@Test
	public void testInjector() throws Exception {
		new HttpHelloWorldLauncher().testInjector();
	}

}