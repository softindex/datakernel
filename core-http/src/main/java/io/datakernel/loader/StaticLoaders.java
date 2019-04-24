package io.datakernel.loader;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.Executor;

public class StaticLoaders {

	private StaticLoaders() {
	}

	public static StaticLoader ofClassPath(Executor executor) {
		return StaticLoaderClassPath.create(executor, null);
	}

	public static StaticLoader ofClassPath(Executor executor, Class<?> relativeClass) {
		return StaticLoaderClassPath.create(executor, relativeClass);
	}

	public static StaticLoader ofClassPath(Executor executor, String relativePath) {
		return StaticLoaderClassPath.create(executor, null, relativePath);
	}

	public static StaticLoader ofClassPath(Executor executor, Class<?> relativeClass, String relativePath) {
		return StaticLoaderClassPath.create(executor, relativeClass, relativePath);
	}

	public static StaticLoader ofPath(Executor executor, Path dir) {
		return SimpleStaticLoaderAsync.create(executor, dir);
	}

	public static StaticLoader ofFile(Executor executor, File dir) {
		return SimpleStaticLoaderAsync.create(executor, dir.toPath());
	}
}
