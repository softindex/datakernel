package io.datakernel.loader;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StaticLoaders {
	private static Executor DEFAULT_EXECUTOR = Executors.newCachedThreadPool();

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

	public static StaticLoader ofClassPath(Class<?> relativeClass) {
		return StaticLoaderClassPath.create(DEFAULT_EXECUTOR, relativeClass);
	}

	public static StaticLoader ofPath(Path dir) {
		return SimpleStaticLoaderAsync.create(dir);
	}

	public static StaticLoader ofFile(File dir) {
		return SimpleStaticLoaderAsync.create(dir.toPath());
	}
}
