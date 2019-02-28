package io.datakernel.loader;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.Executor;

public class StaticLoaders {

	private StaticLoaders() {
	}

	public static StaticLoader ofClassPath(Executor executor) {
		return new StaticLoaderClassPath(executor, null);
	}

	public static StaticLoader ofClassPath(Executor executor, Class<?> relativeClass) {
		return new StaticLoaderClassPath(executor, relativeClass);
	}

	public static StaticLoader ofPath(Executor executor, Path dir) {
		return new SimpleStaticLoaderAsync(executor, dir);
	}

	public static StaticLoader ofFile(Executor executor, File dir) {
		return new SimpleStaticLoaderAsync(executor, dir.toPath());
	}
}
