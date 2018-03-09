package io.datakernel.cube.boot;

import com.google.inject.Module;
import org.junit.Test;

import java.util.Collection;

import static java.util.Collections.singletonList;

public class CubeHttpServerLauncherTest {
	@Test
	public void testInjector() {
		new CubeHttpServerLauncher() {
			@Override
			protected Collection<Module> getCubeModules() {
				return singletonList(new ExampleCubeModule());
			}
		}.testInjector();
	}
}