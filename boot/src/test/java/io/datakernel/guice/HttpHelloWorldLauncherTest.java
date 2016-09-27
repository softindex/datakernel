package io.datakernel.guice;

import org.junit.Test;

public class HttpHelloWorldLauncherTest {
	@Test
	public void getInjector() throws Exception {
		new HttpHelloWorldLauncher().getInjector();
	}

}